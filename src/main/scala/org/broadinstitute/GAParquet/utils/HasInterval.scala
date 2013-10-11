package org.broadinstitute.GAParquet.utils

/**
 * Created with IntelliJ IDEA.
 * User: chartl
 * Date: 10/7/13
 * Time: 4:04 PM
 * To change this template use File | Settings | File Templates.
 */
trait HasInterval {

  def getStart : Long
  def getEnd : Long
  def getReferenceName : String
  def getSize = this.getEnd - this.getStart + 1
}
