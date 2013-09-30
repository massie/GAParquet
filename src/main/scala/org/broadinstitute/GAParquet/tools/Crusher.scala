package org.broadinstitute.GAParquet.tools

import spark.{RDD, SparkContext}
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.ParquetInputFormat
import parquet.avro.AvroReadSupport
import java.io.File
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

/**
 * Created with IntelliJ IDEA.
 * User: chartl
 * Date: 9/26/13
 * Time: 8:04 PM
 * To change this template use File | Settings | File Templates.
 */
abstract class Crusher(files: Seq[File]) {

  def name() : String

  def server() : String = "local"

  def analyze(rdd: RDD[(Void,ADAMRecord)]) : Unit

  def run() : Boolean = {
    val sc = new SparkContext(server(),name())
    val job = new Job()

    ParquetInputFormat.setReadSupportClass(job,classOf[AvroReadSupport[ADAMRecord]])
    // register the input files with Spark
    // note that the "new" doesn't necessairly indicate the creation of a file
    val rdds = files map ( (x: File) => {
      sc.newAPIHadoopFile(x.getAbsolutePath,classOf[ParquetInputFormat[ADAMRecord]],classOf[Void],classOf[ADAMRecord],job.getConfiguration)
    })

    // the RDDs are hooks into the read store, basically the tables themselves. We can go ahead and print the records:
    rdds foreach analyze

    true
  }


}
