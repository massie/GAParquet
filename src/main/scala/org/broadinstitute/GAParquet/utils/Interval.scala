package org.broadinstitute.GAParquet.utils

/**
 * Created with IntelliJ IDEA.
 * User: chartl
 * Date: 10/7/13
 * Time: 4:09 PM
 * To change this template use File | Settings | File Templates.
 */
class Interval(val refName: String, val start: Long, val end: Long, val annotations: Map[String,String]) {
  def this(_ref: String, _start: Long, _end: Long) = this(_ref,_start,_end,null)
  def this(_hasInt: HasInterval) = this(_hasInt.getReferenceName,_hasInt.getStart,_hasInt.getEnd)

  def getStart = start
  def getEnd = end
  def getReferenceName = refName
  def get(annotKey: String) = if ( annotations != null ) annotations(annotKey) else null
  def size = getEnd - getStart + 1
  override def toString = "%s\t%d\t%d" // todo -- annotations are excluded here

}
