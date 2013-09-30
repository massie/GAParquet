package org.broadinstitute.GAParquet.tools

import java.io.File
import spark.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

/**
 * Count reads in an interval
 */
class CountReads(d: Boolean, files: Seq[File], intervals: String) extends Crusher(files) {

  def name() : String = "CountReads"

  def analyze(x: RDD[(Void,ADAMRecord)] ) : Unit = {
    println(x.count())
  }
}
