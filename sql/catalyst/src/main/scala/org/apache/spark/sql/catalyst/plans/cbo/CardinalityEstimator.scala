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

import scala.math.BigDecimal.RoundingMode

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{DecimalType, StringType}


object ExtractAR {
  def unapply(exp: Expression): Option[AttributeReference] = exp match {
    case ar: AttributeReference =>
      Some(ar)
    case Cast(ar: AttributeReference, dataType) =>
      Some(ar)
    case _ =>
      None
  }
}

// method toBigInt() in BigDecimal uses round-down, we need more accurate rounding mode
object RoundingToBigInt {
  def apply(bigValue: BigDecimal): BigInt = {
    if (bigValue > 0 && bigValue <= 1) {
      // do not use rounding if the value is in (0, 1]
      BigInt(1)
    } else {
      val tmp = bigValue.setScale(0, RoundingMode.HALF_UP)
      tmp.toBigInt()
    }
  }
}

// utils for estimation
object EstimationUtils {

  def isPrimaryKey(rowsPerDv: BigDecimal): Boolean = {
    rowsPerDv > 0.95 && rowsPerDv < 1.05
  }

  def findFirstBucketForValue(value: Double, histogram: Array[NumericEquiHeightBin]): Int = {
    var bucketId = 0
    histogram.foreach { bucket =>
      if (value > bucket.x) bucketId += 1
    }
    bucketId
  }

  def findLastBucketForValue(value: Double, histogram: Array[NumericEquiHeightBin]): Int = {
    var bucketId = 0
    histogram.foreach { bucket =>
      if (value >= bucket.x && bucketId < histogram.length - 1) {
        bucketId += 1
      }
    }
    bucketId
  }

  private def getOccupation(
      bucketId: Int,
      higherValue: Double,
      lowerValue: Double,
      histogram: Array[NumericEquiHeightBin],
      hgmMin: Double): Double = {
    val curBucket = histogram(bucketId)
    if (bucketId == 0 && curBucket.x == hgmMin) {
      // the Min of the histogram occupy the whole first bucket
      1.0
    } else if (bucketId == 0 && curBucket.x != hgmMin) {
      if (higherValue == lowerValue) {
        // in the case curBucket.bucketNdv == 0, current bucket is occupied by one value, which
        // is included in the previous bucket
        1.0 / math.max(curBucket.bucketNdv.toDouble, 1)
      } else {
        (higherValue - lowerValue) / (curBucket.x - hgmMin)
      }
    } else {
      val preBucket = histogram(bucketId - 1)
      if (curBucket.x == preBucket.x) {
        1.0
      } else if (higherValue == lowerValue) {
        1.0 / math.max(curBucket.bucketNdv.toDouble, 1)
      } else {
        (higherValue - lowerValue) / (curBucket.x - preBucket.x)
      }
    }
  }

  def getOccupationBuckets(
      higherEnd: Double,
      lowerEnd: Double,
      histogram: Array[NumericEquiHeightBin],
      hgmMin: Double): Double = {
    // find buckets where current min and max locate
    val minInBucketId = findFirstBucketForValue(lowerEnd, histogram)
    val maxInBucketId = findLastBucketForValue(higherEnd, histogram)
    assert(minInBucketId <= maxInBucketId)

    // compute how much current [min, max] occupy the histogram, in the number of buckets
    getOccupationBuckets(maxInBucketId, minInBucketId, higherEnd, lowerEnd, histogram, hgmMin)
  }

  def getOccupationBuckets(
      higherId: Int,
      lowerId: Int,
      higherEnd: Double,
      lowerEnd: Double,
      histogram: Array[NumericEquiHeightBin],
      hgmMin: Double): Double = {
    if (lowerId == higherId) {
      getOccupation(lowerId, higherEnd, lowerEnd, histogram, hgmMin)
    } else {
      // compute how much lowerEnd/higherEnd occupy its bucket
      val lowerCurBucket = histogram(lowerId)
      val lowerPart = getOccupation(lowerId, lowerCurBucket.x, lowerEnd, histogram, hgmMin)

      // in case higherId > lowerId, higherId must be > 0
      val higherPreBucket = histogram(higherId - 1)
      val higherPart = getOccupation(higherId, higherEnd, higherPreBucket.x, histogram, hgmMin)
      // the total length is lowerPart + higherPart + buckets between them
      higherId - lowerId - 1 + lowerPart + higherPart
    }
  }

  def getOccupationNdv(
      higherId: Int,
      lowerId: Int,
      higherEnd: Double,
      lowerEnd: Double,
      histogram: Array[NumericEquiHeightBin],
      hgmMin: Double): Long = {
    val ndv: Double = if (higherEnd == lowerEnd) {
      1
    } else if (lowerId == higherId) {
      getOccupation(lowerId, higherEnd, lowerEnd, histogram, hgmMin) * histogram(lowerId).bucketNdv
    } else {
      // compute how much lowerEnd/higherEnd occupy its bucket
      val minCurBucket = histogram(lowerId)
      val minPartNdv = getOccupation(lowerId, minCurBucket.x, lowerEnd, histogram, hgmMin) *
        minCurBucket.bucketNdv

      // in case higherId > lowerId, higherId must be > 0
      val maxCurBucket = histogram(higherId)
      val maxPreBucket = histogram(higherId - 1)
      val maxPartNdv = getOccupation(higherId, higherEnd, maxPreBucket.x, histogram, hgmMin) *
        maxCurBucket.bucketNdv

      // The total ndv is minPartNdv + maxPartNdv + Ndvs between them.
      // Note that we need to handle popular values crossing many buckets,
      // their ndv can only be counted once.
      var middleNdv: Long = 0
      var previous = hgmMin
      for (i <- histogram.indices) {
        val bucket = histogram(i)
        if (bucket.x != previous && i >= lowerId + 1 && i <= higherId - 1) {
          middleNdv += bucket.bucketNdv
        }
        previous = bucket.x
      }
      minPartNdv + maxPartNdv + middleNdv
    }
    math.round(ndv)
  }

}
