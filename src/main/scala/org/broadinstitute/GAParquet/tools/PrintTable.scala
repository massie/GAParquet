package org.broadinstitute.GAParquet.tools

import spark.{SparkContext,RDD}
import org.apache.hadoop.mapreduce.Job
import parquet.avro.{AvroParquetOutputFormat,AvroWriteSupport,AvroReadSupport}
import parquet.filter.{RecordFilter,UnboundRecordFilter}
import parquet.column.ColumnReader
import parquet.filter.ColumnRecordFilter._
import parquet.filter.ColumnPredicates._
import java.io.File
import parquet.hadoop.ParquetInputFormat
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

/**
 * Takes the input ADAM-formatted parquet data and displays the table
 */
class PrintTable(debug: Boolean, inputFiles: Seq[File]) extends Crusher {

  def run() : Boolean = {
    val sc = new SparkContext("local","PrintTable")
    val job = new Job()

    ParquetInputFormat.setReadSupportClass(job,classOf[AvroReadSupport[ADAMRecord]])
    // register the input files with Spark
    // note that the "new" doesn't necessairly indicate the creation of a file
    val rdds = inputFiles map ( (x: File) => {
      sc.newAPIHadoopFile(x.getAbsolutePath,classOf[ParquetInputFormat[ADAMRecord]],classOf[Void],classOf[ADAMRecord],job.getConfiguration)
    })

    // the RDDs are hooks into the read store, basically the tables themselves. We can go ahead and print the records:
    rdds foreach ( (x: RDD[(Void,ADAMRecord)]) => {
      println("-------------------")
      println(x.id)
      println("-------------------")
      x foreach ( (y: (Void,ADAMRecord) ) => {
        println(y._2.toString)
      })
    })

    return true
  }
}
