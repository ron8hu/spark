/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.plans.cbo

import java.util.regex.Pattern

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataType, DateType, NumericType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String


object FilterCardEstimator extends Logging {

  private val delta: Double = 0.000000001

  def apply(plan: Filter): Statistics = {
    val stats: Statistics = Statistics(plan.child.statistics.sizeInBytes,
        plan.child.statistics.rowCount, plan.child.statistics.colStats,
        plan.child.statistics.isBroadcastable)
    if (plan.statistics.rowCount.isEmpty) {
      return Statistics(0, Some(0), Map.empty, isBroadcastable = false)
    }

    // estimate selectivity for this filter
    val percent: Double = calculateConditions(plan.statistics, plan.condition)
    val filteredSizeInBytes = RoundingToBigInt(BigDecimal(plan.statistics.sizeInBytes) * percent)
    val filteredRowCount = plan.statistics.rowCount.map(
      r => RoundingToBigInt(BigDecimal(r) * percent)
    )
    // CardinalityEstimator.updateStatsByOutput(plan, plan.statistics, plan.statistics
    //   .histograms)
    // CardinalityEstimator.updateNdvBasedOnRows(plan.statistics)

    stats.copy(sizeInBytes = filteredSizeInBytes, rowCount = filteredRowCount)
  }

  def calculateConditions(
      planStat: Statistics,
      condition: Expression,
      update: Boolean = true): Double = {
    // For conditions linked by And, we can update stats (only basicStats for columns)
    // after estimation, so that the stats will be more accurate for following estimation.
    condition match {
      case And(cond1, cond2) =>
        calculateConditions(planStat, cond1, update) * calculateConditions(planStat, cond2, update)
      case Or(cond1, cond2) =>
        math.min(1.0, calculateConditions(planStat, cond1, update = false) +
          calculateConditions(planStat, cond2, update = false))
      case Not(cond) => calculateSingleCondition(planStat, cond, isNot = true, update = false)
      case _ => calculateSingleCondition(planStat, condition, isNot = false, update)
    }
  }

  def calculateSingleCondition(
      planStat: Statistics,
      condition: Expression,
      isNot: Boolean,
      update: Boolean): Double = {
    var notSupported: Boolean = false
    val percent: Double = condition match {
      // Currently we only support binary predicates where one side is a column,
      // and the other is a literal.
      // Note that: all binary predicate computing methods assume the literal is at the right side,
      // so we will change the predicate order if not.
      case op @ LessThan(ExtractAR(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op @ LessThan(l: Literal, ExtractAR(ar)) =>
        evaluateBinary(GreaterThan(ar, l), planStat, ar, l, update)

      case op @ LessThanOrEqual(ExtractAR(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op @ LessThanOrEqual(l: Literal, ExtractAR(ar)) =>
        evaluateBinary(GreaterThanOrEqual(ar, l), planStat, ar, l, update)

      case op @ GreaterThan(ExtractAR(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op @ GreaterThan(l: Literal, ExtractAR(ar)) =>
        evaluateBinary(LessThan(ar, l), planStat, ar, l, update)

      case op @ GreaterThanOrEqual(ExtractAR(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op @ GreaterThanOrEqual(l: Literal, ExtractAR(ar)) =>
        evaluateBinary(LessThanOrEqual(ar, l), planStat, ar, l, update)

      // EqualTo does not care about the order
      case op @ EqualTo(ExtractAR(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op @ EqualTo(l: Literal, ExtractAR(ar)) =>
        evaluateBinary(op, planStat, ar, l, update)

      case In(ExtractAR(ar), expList) if !expList.exists(!_.isInstanceOf[Literal]) =>
        // Expression [In (value, seq[Literal])] will be replaced with optimized version
        // [InSet (value, HashSet[Literal])] in Optimizer, but only for list.size > 10.
        // Here we convert In into InSet anyway, because they share the same processing logic.
        val hSet = expList.map(e => e.eval())
        evaluateInSet(planStat, ar, HashSet() ++ hSet, update)
      case InSet(ExtractAR(ar), set) =>
        evaluateInSet(planStat, ar, set, update)

      case Like(_, _) | Contains(_, _) | StartsWith(_, _) | EndsWith(_, _) =>
        evaluateLike(condition, planStat, update)

      // TODO: it's difficult to estimate IsNull after outer joins
//      case IsNull(ExtractAR(ar)) =>
//        evaluateIsNull(planStat, ar, update)
      case IsNotNull(ExtractAR(ar)) =>
        evaluateIsNotNull(planStat, ar, update)

//      case op @ EqualNullSafe(ExtractAR(ar), l: Literal) =>
//      case op @ EqualNullSafe(l: Literal, ExtractAR(ar)) =>

      case _ =>
        logDebug("[CBO] Unsupported filter condition: " + condition)
        notSupported = true
        1.0
    }
    if (notSupported) {
      1.0
    } else if (isNot) {
      1.0 - percent
    } else {
      percent
    }
  }

  def evaluateIsNotNull(
      planStat: Statistics,
      attrRef: AttributeReference,
      update: Boolean): Double = {
    if (!planStat.basicStats.contains(attrRef)) {
      logInfo("[CBO] No statistics for " + attrRef)
      return 1.0
    }
    val colStats = planStat.basicStats(attrRef)
    val percent = BigDecimal(colStats.numNull) / BigDecimal(planStat.rowCount)
    if (update) {
      val newStats = colStats match {
        case sbs: StringBasicStat => sbs.copy(numNull = 0)
        case nbs: NumericBasicStat => nbs.copy(numNull = 0)
      }
      planStat.basicStats += (attrRef -> newStats)
    }
    1.0 - percent.toDouble
  }

  def evaluateBinary(
      op: BinaryComparison,
      planStat: Statistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean): Double = {
    if (!planStat.basicStats.contains(attrRef)) {
      logInfo("[CBO] No statistics for " + attrRef)
      return 1.0
    }
    op match {
      case EqualTo(l, r) => evaluateEqualTo(op, planStat, attrRef, literal, update)
      case _ =>
        attrRef.dataType match {
          case StringType => evaluateBinaryForString(op, planStat, attrRef, literal, update)
          case dataType: DataType if (dataType.isInstanceOf[NumericType]
            || dataType.isInstanceOf[DateType]
            || dataType.isInstanceOf[TimestampType]) =>
            evaluateBinaryForNumeric(op, planStat, attrRef, literal, update)
        }
    }
  }

  def evaluateBinaryForNumeric(
      op: BinaryComparison,
      planStat: Statistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean): Double = {

    var percent = 1.0
    val colStats = planStat.basicStats(attrRef).asInstanceOf[NumericBasicStat]
    val max = colStats.max
    val min = colStats.min
    val ndv = colStats.ndv
    val datum = attrRef.dataType match {
      case d: DateType if literal.dataType.isInstanceOf[StringType] =>
        val date = DateTimeUtils.stringToDate(literal.value.asInstanceOf[UTF8String])
        if (date.isEmpty) {
          logInfo("[CBO] Date literal is wrong, No statistics for " + attrRef)
          return 1.0
        }
        date.get.toDouble
      case t: TimestampType if literal.dataType.isInstanceOf[StringType] =>
        val timestamp = DateTimeUtils.stringToTimestamp(literal.value.asInstanceOf[UTF8String])
        if (timestamp.isEmpty) {
          logInfo("[CBO] Timestamp literal is wrong, No statistics for " + attrRef)
          return 1.0
        }
        timestamp.get.toDouble
      case _ => literal.value.toString.toDouble
    }

    val (noOverlap: Boolean, completeOverlap: Boolean) = op match {
      case LessThan(l, r) => (datum <= min, datum > max)
      case LessThanOrEqual(l, r) => (datum < min, datum >= max)
      case GreaterThan(l, r) => (datum >= max, datum < min)
      case GreaterThanOrEqual(l, r) => (datum > max, datum <= min)
    }

    if (noOverlap) {
      percent = 0.0
    } else if (completeOverlap) {
      percent = 1.0
    } else {
      var newHgm: Array[Bucket] = null
      var newMax = max
      var newMin = min
      var newNdv = ndv
      if (planStat.histograms.contains(attrRef)) {
        val histogram = planStat.histograms(attrRef)
        histogram.head match {
          case b: NumericEquiWidthBucket =>
            val result = computePercentForEquiWidthHgm(op, histogram, max.toString,
              min.toString, datum.toString)
            percent = result._1
            // this new equi-width histogram is accurate
            newHgm = result._2
            val bucketValues = newHgm.map(_.x.toString.toDouble)
            newMax = bucketValues.max
            newMin = bucketValues.min
            newNdv = bucketValues.length
          case b: NumericEquiHeightBucket =>
            val histogramMin: Double = planStat.equiHeightHgmMins(attrRef)
            val equiHeightHgm = histogram.map(_.asInstanceOf[NumericEquiHeightBucket])
            percent = computePercentForNumericEquiHeightHgm(op, equiHeightHgm, histogramMin, max,
              min, datum)
            op match {
              case GreaterThan(l, r) =>
                // Since for equi-height histogram, we can't find the exact smallest value which is
                // greater than or equal to min, we just make a fake value using a very small delta.
                newMin = datum + delta
              case GreaterThanOrEqual(l, r) => newMin = datum
              case LessThan(l, r) =>
                // we also make a fake value using a very small delta
                newMax = datum - delta
              case LessThanOrEqual(l, r) => newMax = datum
            }
            newNdv = computeNewNdvForNumericEquiHeightHgm(op, equiHeightHgm, histogramMin, newMax,
              newMin)
        }
      } else {
        // max != min, if not, it will be filtered in previous two branches
        percent = op match {
          case LessThan(l, r) =>
            (datum - min) / (max - min)
          case LessThanOrEqual(l, r) =>
            if (datum == min) 1.0 / ndv.toDouble
            else (datum - min) / (max - min)
          case GreaterThan(l, r) =>
            (max - datum) / (max - min)
          case GreaterThanOrEqual(l, r) =>
            if (datum == max) 1.0 / ndv.toDouble
            else (max - datum) / (max - min)
        }
        op match {
          case GreaterThan(l, r) => newMin = datum + delta
          case GreaterThanOrEqual(l, r) => newMin = datum
          case LessThan(l, r) => newMax = datum - delta
          case LessThanOrEqual(l, r) => newMax = datum
        }
        newNdv = math.max(math.round(ndv * percent), 1)
      }
      if (update) {
        // update histogram if we have a new one
        if (newHgm != null) {
          planStat.histograms += (attrRef -> newHgm)
        }
        val newStats = NumericBasicStat(newMax, newMin, newNdv, 0)
        planStat.basicStats += (attrRef -> newStats)
      }
    }
    percent
  }

  def evaluateBinaryForString(
      op: BinaryComparison,
      planStat: PlanStatistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean): Double = {

    var percent = 1.0
    val colStats = planStat.basicStats(attrRef).asInstanceOf[StringBasicStat]
    val max = colStats.max
    val min = colStats.min
    val ndv = colStats.ndv
    val datum = literal.value.toString

    val (noOverlap: Boolean, completeOverlap: Boolean) = op match {
      case LessThan(l, r) => (datum <= min, datum > max)
      case LessThanOrEqual(l, r) => (datum < min, datum >= max)
      case GreaterThan(l, r) => (datum >= max, datum < min)
      case GreaterThanOrEqual(l, r) => (datum > max, datum <= min)
    }

    if (noOverlap) {
      percent = 0.0
    } else if (completeOverlap) {
      percent = 1.0
    } else {
      var newHgm: Array[Bucket] = null
      var newMax = max
      var newMin = min
      var newNdv = ndv
      if (planStat.histograms.contains(attrRef)) {
        // for string type, histogram must be equi-width
        val result = computePercentForEquiWidthHgm(op, planStat.histograms(attrRef), max, min,
          datum)
        percent = result._1
        newHgm = result._2
        val bucketValues = newHgm.map(_.x.toString)
        newMax = bucketValues.max
        newMin = bucketValues.min
        newNdv = bucketValues.length
      } else {
        // do not estimate
        percent = 1.0
      }
      if (update) {
        // update histogram if we have a new one
        if (newHgm != null) {
          planStat.histograms += (attrRef -> newHgm)
        }
        val newStats = StringBasicStat(newMax, newMin, newNdv, 0, colStats.maxLength,
          colStats.avgLength)
        planStat.basicStats += (attrRef -> newStats)
      }
    }
    percent
  }

  // compute ndv in the newly updated [lower, upper]
  def computeNewNdvForNumericEquiHeightHgm(
      op: BinaryComparison,
      histogram: Array[NumericEquiHeightBucket],
      histogramMin: Double,
      upper: Double,
      lower: Double): Long = {
    // find buckets where current min and max locate
    val lowerInBucketId = EstimationUtils.findFirstBucketForValue(lower, histogram)
    val upperInBucketId = EstimationUtils.findLastBucketForValue(upper, histogram)
    assert(lowerInBucketId <= upperInBucketId)
    EstimationUtils.getOccupationNdv(upperInBucketId, lowerInBucketId, upper, lower, histogram,
      histogramMin)
  }

  def computePercentForNumericEquiHeightHgm(
      op: BinaryComparison,
      histogram: Array[NumericEquiHeightBucket],
      histogramMin: Double,
      max: Double,
      min: Double,
      datumNumber: Double): Double = {
    // find buckets where current min and max locate
    val minInBucketId = EstimationUtils.findFirstBucketForValue(min, histogram)
    val maxInBucketId = EstimationUtils.findLastBucketForValue(max, histogram)
    assert(minInBucketId <= maxInBucketId)

    // compute how much current [min, max] occupy the histogram, in the number of buckets
    val minToMaxLength = EstimationUtils.getOccupationBuckets(maxInBucketId, minInBucketId, max,
      min, histogram, histogramMin)

    val datumInBucketId = op match {
      case LessThan(_, _) | GreaterThanOrEqual(_, _) =>
        EstimationUtils.findFirstBucketForValue(datumNumber, histogram)
      case LessThanOrEqual(_, _) | GreaterThan(_, _) =>
        EstimationUtils.findLastBucketForValue(datumNumber, histogram)
    }

    op match {
      // LessThan and LessThanOrEqual share the same logic,
      // but their datumInBucketId may be different
      case LessThan(_, _) | LessThanOrEqual(_, _) =>
        EstimationUtils.getOccupationBuckets(datumInBucketId, minInBucketId, datumNumber, min,
          histogram, histogramMin) / minToMaxLength
      // GreaterThan and GreaterThanOrEqual share the same logic,
      // but their datumInBucketId may be different
      case GreaterThan(_, _) | GreaterThanOrEqual(_, _) =>
        EstimationUtils.getOccupationBuckets(maxInBucketId, datumInBucketId, max, datumNumber,
          histogram, histogramMin) / minToMaxLength
    }
  }

  // equi-width histograms share the same computing logic
  def computePercentForEquiWidthHgm(
      op: BinaryComparison,
      histogram: Array[Bucket],
      upper: String,
      lower: String,
      datum: String): (Double, Array[Bucket]) = {
    // need to count numbers here because max/min can be changed during estimation
    var rangeCount: Double = 0
    var validCount: Double = 0
    val equiWidthHgm = histogram.map(_.asInstanceOf[EquiWidthBucket])
    val hgmAfterJoin: ArrayBuffer[EquiWidthBucket] = new ArrayBuffer[EquiWidthBucket]()
    op match {
      case LessThan(l, r) =>
        // for LessThan, upperbound is exclusive
        equiWidthHgm.foreach { bucket =>
          if (bucket.ge(lower) && bucket.le(upper)) rangeCount += bucket.frequency
          if (bucket.ge(lower) && bucket.lt(datum)) {
            validCount += bucket.frequency
            hgmAfterJoin += bucket
          }
        }
      case LessThanOrEqual(l, r) =>
        // for LessThanOrEqual, upperbound is inclusive
        equiWidthHgm.foreach { bucket =>
          if (bucket.ge(lower) && bucket.le(upper)) rangeCount += bucket.frequency
          if (bucket.ge(lower) && bucket.le(datum)) {
            validCount += bucket.frequency
            hgmAfterJoin += bucket
          }
        }
      case GreaterThan(l, r) =>
        // for GreaterThan, lowerbound is exclusive
        equiWidthHgm.foreach { bucket =>
          if (bucket.ge(lower) && bucket.le(upper)) rangeCount += bucket.frequency
          if (bucket.gt(datum) && bucket.le(upper)) {
            validCount += bucket.frequency
            hgmAfterJoin += bucket
          }
        }
      case GreaterThanOrEqual(l, r) =>
        // for GreaterThanOrEqual, lowerbound is inclusive
        equiWidthHgm.foreach { bucket =>
          if (bucket.ge(lower) && bucket.le(upper)) rangeCount += bucket.frequency
          if (bucket.ge(datum) && bucket.le(upper)) {
            validCount += bucket.frequency
            hgmAfterJoin += bucket
          }
        }
      case EqualTo(l, r) =>
        equiWidthHgm.foreach { bucket =>
          if (bucket.ge(lower) && bucket.le(upper)) rangeCount += bucket.frequency
          if (bucket.eq(datum)) {
            validCount += bucket.frequency
            hgmAfterJoin += bucket
          }
        }
    }
    // since min <= max, rangeCount will never be zero
    val percent = if (rangeCount == 0) 0 else validCount / rangeCount
    (percent, hgmAfterJoin.toArray)
  }

  def evaluateEqualTo(
      op: BinaryComparison,
      planStat: PlanStatistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean): Double = {

    var percent: Double = 1.0
    val colStats = planStat.basicStats(attrRef)
    val maxString = colStats.max.toString
    val minString = colStats.min.toString
    val ndv = colStats.ndv
    val datumString = attrRef.dataType match {
      case d: DateType if literal.dataType.isInstanceOf[StringType] =>
        val date = DateTimeUtils.stringToDate(literal.value.asInstanceOf[UTF8String])
        if (date.isEmpty) {
          logInfo("[CBO] Date literal is wrong, No statistics for " + attrRef)
          return 1.0
        }
        date.get.toString
      case t: TimestampType if literal.dataType.isInstanceOf[StringType] =>
        val timestamp = DateTimeUtils.stringToTimestamp(literal.value.asInstanceOf[UTF8String])
        if (timestamp.isEmpty) {
          logInfo("[CBO] Timestamp literal is wrong, No statistics for " + attrRef)
          return 1.0
        }
        timestamp.get.toString
      case _ => literal.value.toString
    }

    // decide if the value is in [min, max] of the column
    val inBoundary: Boolean = attrRef.dataType match {
      case StringType =>
        datumString >= minString && datumString <= maxString
      case dataType: DataType if (dataType.isInstanceOf[NumericType]
        || dataType.isInstanceOf[DateType]
        || dataType.isInstanceOf[TimestampType]) =>
        datumString.toDouble >= minString.toDouble && datumString.toDouble <= maxString.toDouble
    }
    if (inBoundary) {
      var newHgm: Array[Bucket] = null
      if (planStat.histograms.contains(attrRef)) {
        val histogram = planStat.histograms(attrRef)
        histogram.head match {
          // for equi-width histogram, the result percent is accurate
          case b: EquiWidthBucket =>
            val result = computePercentForEquiWidthHgm(op, histogram, maxString, minString,
              datumString)
            percent = result._1
            newHgm = result._2
          case b: NumericEquiHeightBucket =>
            val datum = datumString.toDouble
            // find the interval where this datum locates
            var lowerId, higherId = -1
            for (i <- histogram.indices) {
              val nehBucket = histogram(i).asInstanceOf[NumericEquiHeightBucket]
              // if datum > x, just proceed and do nothing
              if (datum <= nehBucket.x && lowerId < 0) lowerId = i
              if ((datum < nehBucket.x || i == histogram.length - 1) && higherId < 0) higherId = i
            }
            assert(lowerId <= higherId)
            val lowerBucNdv = histogram(lowerId).asInstanceOf[NumericEquiHeightBucket].bucketNdv
            val higherBucNdv = histogram(higherId).asInstanceOf[NumericEquiHeightBucket].bucketNdv
            // assume uniform distribution in each bucket
            percent = if (lowerId == higherId) {
              1.0 / histogram.length * 1.0 / math.max(lowerBucNdv, 1)
            } else {
              1.0 / histogram.length * (higherId - lowerId - 1) +
                1.0 / histogram.length * 1.0 / math.max(lowerBucNdv, 1) +
                1.0 / histogram.length * 1.0 / math.max(higherBucNdv, 1)
            }
        }
      } else {
        percent = 1.0 / ndv.toDouble
      }

      if (update) {
        if (newHgm != null) {
          planStat.histograms += (attrRef -> newHgm)
        }
        val newStats = attrRef.dataType match {
          case StringType =>
            StringBasicStat(datumString, datumString, 1, 0, datumString.length, datumString.length)
          case dataType: DataType if (dataType.isInstanceOf[NumericType]
            || dataType.isInstanceOf[DateType]
            || dataType.isInstanceOf[TimestampType]) =>
            NumericBasicStat(datumString.toDouble, datumString.toDouble, 1, 0)
        }
        planStat.basicStats += (attrRef -> newStats)
      }
    } else {
      percent = 0.0
    }
    percent
  }

  def evaluateInSet(
      planStat: PlanStatistics,
      attrRef: AttributeReference,
      hSet: Set[Any],
      update: Boolean): Double = {
    if (!planStat.basicStats.contains(attrRef)) {
      logInfo("[CBO] No statistics for " + attrRef)
      return 1.0
    }

    var percent: Double = 1.0
    val colStats = planStat.basicStats(attrRef)
    val max = colStats.max.toString
    val min = colStats.min.toString
    val ndv = colStats.ndv

    // use [min, max] to filter the original hSet
    val validQuerySet = attrRef.dataType match {
      case StringType =>
        hSet.map(e => e.toString).filter(e => e >= min && e <= max)
      case dataType: DataType if (dataType.isInstanceOf[NumericType]
        || dataType.isInstanceOf[DateType]
        || dataType.isInstanceOf[TimestampType]) =>
        hSet.map(e => e.toString.toDouble).filter(e => e >= min.toDouble && e <= max.toDouble)
    }
    if (validQuerySet.isEmpty) {
      return 0.0
    }
    val newHgm: ArrayBuffer[EquiWidthBucket] = new ArrayBuffer[EquiWidthBucket]()
    val (newMax: String, newMin: String, newNdv) = if (planStat.histograms.contains(attrRef) &&
      planStat.histograms(attrRef).head.isInstanceOf[EquiWidthBucket]) {
      // with equi-width histogram, we can get accurate results
      val histogram = planStat.histograms(attrRef)
      var finalSet: Set[Any] = HashSet()
      var totalCount: Double = 0
      var finalCount: Double = 0
      histogram.foreach { b =>
        val bucket = b.asInstanceOf[EquiWidthBucket]
        if (bucket.ge(min) && bucket.le(max)) totalCount += bucket.frequency
        if (validQuerySet.exists(e => bucket.eq(e.toString))) {
          finalSet += bucket.x
          finalCount += bucket.frequency
          newHgm += bucket
        }
      }
      percent = if (totalCount == 0) {
        0.0
      } else {
        finalCount / totalCount
      }
      if (finalSet.isEmpty) {
        return 0.0
      }
      attrRef.dataType match {
        case StringType =>
          val tmpSet: Set[String] = finalSet.map(e => e.toString)
          (tmpSet.max, tmpSet.min, tmpSet.size)
        case dataType: DataType if (dataType.isInstanceOf[NumericType]
          || dataType.isInstanceOf[DateType]
          || dataType.isInstanceOf[TimestampType]) =>
          val tmpSet: Set[Double] = finalSet.map(e => e.toString.toDouble)
          (tmpSet.max.toString, tmpSet.min.toString, tmpSet.size)
      }
    } else {
      // just use basic stats, assume validSet is a subset of values
      percent = math.min(1.0, validQuerySet.size / ndv.toDouble)

      attrRef.dataType match {
        case StringType =>
          val tmpSet: Set[String] = validQuerySet.map(e => e.toString)
          (tmpSet.max, tmpSet.min, tmpSet.size)
        case dataType: DataType if (dataType.isInstanceOf[NumericType]
          || dataType.isInstanceOf[DateType]
          || dataType.isInstanceOf[TimestampType]) =>
          val tmpSet: Set[Double] = validQuerySet.map(e => e.toString.toDouble)
          (tmpSet.max.toString, tmpSet.min.toString, tmpSet.size)
      }
    }

    if (update) {
      if (newHgm.nonEmpty) {
        planStat.histograms += (attrRef -> newHgm.toArray)
      }
      val newStats = attrRef.dataType match {
        case StringType =>
          val stat = colStats.asInstanceOf[StringBasicStat]
          StringBasicStat(newMax, newMin, newNdv, 0, stat.maxLength, stat.avgLength)
        case dataType: DataType if (dataType.isInstanceOf[NumericType]
          || dataType.isInstanceOf[DateType]
          || dataType.isInstanceOf[TimestampType]) =>
          NumericBasicStat(newMax.toDouble, newMin.toDouble, newNdv, 0)
      }
      planStat.basicStats += (attrRef -> newStats)
    }
    percent
  }

  def evaluateLike(cond: Expression, planStat: PlanStatistics, update: Boolean): Double = {
    if (!cond.asInstanceOf[BinaryExpression].left.isInstanceOf[AttributeReference]) {
      logDebug("[CBO] Unsupported like condition: " + cond)
      return 1.0
    }
    val attrRef = cond.asInstanceOf[BinaryExpression].left.asInstanceOf[AttributeReference]
    if (!planStat.basicStats.contains(attrRef)) {
      logInfo("[CBO] No statistics for " + attrRef)
      return 1.0
    }
    if (!planStat.histograms.contains(attrRef) ||
      !planStat.histograms(attrRef).head.isInstanceOf[StringEquiWidthBucket]) {
      logInfo("[CBO] No histogram for string attribute: " + attrRef)
      return 1.0
    }

    val colStats = planStat.basicStats(attrRef).asInstanceOf[StringBasicStat]
    var totalCount: Double = 0
    var validCount: Double = 0
    var validSet: Set[String] = HashSet()
    val newHgm: ArrayBuffer[EquiWidthBucket] = new ArrayBuffer[EquiWidthBucket]()
    cond match {
      case like @ Like(_, _) =>
        val rightStr = like.right.eval().asInstanceOf[UTF8String].toString
        val regex = Pattern.compile(like.escape(rightStr))
        planStat.histograms(attrRef).foreach { b =>
          val bucket = b.asInstanceOf[StringEquiWidthBucket]
          if (bucket.x >= colStats.min && bucket.x <= colStats.max) totalCount += bucket.frequency

          if (like.matches(regex, bucket.x)) {
            validSet += bucket.x
            validCount += bucket.frequency
            newHgm += bucket
          }
        }
      case biExp: BinaryExpression =>
        // Like will be simplified if it can be converted into Contains/StartsWith/EndsWith
        val compareFunc: (UTF8String, UTF8String) => Boolean = biExp match {
          case contains @ Contains(l, r) => contains.compare
          case startsWith @ StartsWith(l, r) => startsWith.compare
          case endsWith @ EndsWith(l, r) => endsWith.compare
        }
        val rightStr = biExp.right.eval().asInstanceOf[UTF8String]
        planStat.histograms(attrRef).foreach { b =>
          val bucket = b.asInstanceOf[StringEquiWidthBucket]
          if (bucket.x >= colStats.min && bucket.x <= colStats.max) totalCount += bucket.frequency

          val valueStr = UTF8String.fromString(bucket.x)
          if (compareFunc(valueStr, rightStr)) {
            validSet += bucket.x
            validCount += bucket.frequency
            newHgm += bucket
          }
        }
    }
    if (validSet.isEmpty) {
      return 0
    }
    if (update) {
      if (newHgm.nonEmpty) {
        planStat.histograms += (attrRef -> newHgm.toArray)
      }
      val newStats = StringBasicStat(validSet.max, validSet.min, validSet.size, 0,
        colStats.maxLength, colStats.avgLength)
      planStat.basicStats += (attrRef -> newStats)
    }
    if (totalCount == 0) 0.0 else validCount / totalCount
  }
}
