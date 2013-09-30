package org.broadinstitute.GAParquet.tools

import spark.RDD
import java.io.File
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

/**
 * Takes the input ADAM-formatted parquet data and displays the table
 */
class PrintTable(debug: Boolean, inputFiles: Seq[File]) extends Crusher(inputFiles) {

  def analyze(x: RDD[(Void,ADAMRecord)]) : Unit = {
    println("-------------------")
    println(x.id)
    println("-------------------")
    x foreach ( (y: (Void,ADAMRecord) ) => {
      println(y._2.toString)
    })
  }

  def name() : String = "PrintTable"
}
