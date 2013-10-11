package org.broadinstitute.GAParquet.utils

import scala.collection.mutable

/**
 * A searchable interval set, constrained to be within one contig
 */
class IntervalSet(_intervals : Seq[Interval]) extends Iterable[Interval]{

  val intervals : Array[Interval] = _intervals.toArray
  val startPoints : Array[Long] = intervals.map(_.getStart)
  val endPoints : Array[Long] = intervals.map(_.getEnd)

  def iterator = intervals.iterator

  def getOverlapping(int: HasInterval) : Seq[Interval] = {
    // an overlapping interval cannot start after int.end, so get those that do
    val firstStartAfterEnd = java.util.Arrays.binarySearch(startPoints,int.getEnd)
    // an overlapping interval cannot end before int.start, so get those that do
    val firstEndBeforeStart = java.util.Arrays.binarySearch(endPoints,int.getStart)
    // number of hits is the difference
    val numOverlapping = math.abs(firstStartAfterEnd) - math.abs(firstEndBeforeStart)
    // now return the intervals falling into the offsets between the found indexes
    val start_offset = if ( firstEndBeforeStart < 0 ) 1 + math.abs(firstEndBeforeStart) else firstEndBeforeStart
    val end_offset = if ( firstStartAfterEnd < 0 ) math.abs(firstStartAfterEnd) - 1 else firstStartAfterEnd

    (start_offset to end_offset).map(intervals(_))
  }

}
