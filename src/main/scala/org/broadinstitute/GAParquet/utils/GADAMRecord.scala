package org.broadinstitute.GAParquet.utils

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

/**
 * Created with IntelliJ IDEA.
 * User: chartl
 * Date: 10/7/13
 * Time: 4:06 PM
 * To change this template use File | Settings | File Templates.
 */
class GADAMRecord(val record: ADAMRecord) extends HasInterval {
  // container for the adam record

  def getStart = record.getStart
  def getEnd = record.getEnd
  def getReferenceName = record.getReferenceName.asInstanceOf[String]

}
