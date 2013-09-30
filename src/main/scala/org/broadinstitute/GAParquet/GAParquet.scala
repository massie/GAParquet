import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import parquet.hadoop.{ParquetOutputFormat,ParquetInputFormat}
import java.io.File
import spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import parquet.avro.{AvroParquetOutputFormat,AvroWriteSupport,AvroReadSupport}
import parquet.filter.{RecordFilter,UnboundRecordFilter}
import parquet.column.ColumnReader
import parquet.filter.ColumnRecordFilter._
import parquet.filter.ColumnPredicates._
import com.beust.jcommander.JCommander

package org.broadinstitute.GAParquet {

import scala.collection.JavaConversions._
import org.broadinstitute.GAParquet.tools.{CountReads, PrintTable, Crusher}
import com.beust.jcommander.Parameter

object GAParquet {

  object GAPConfig {
      // Declared as var because JCommander assigns a new collection declared
      // as java.util.List because that's what JCommander will replace it with.
      // It'd be nice if JCommander would just use the provided List so this
      // could be a val and a Scala LinkedList.
      @Parameter(names = Array("-I", "--input"),description = "Files to load. Can be specified multiple times.")
      var inFiles: java.util.List[String] = null

      @Parameter(names=Array("-D","--debug"),description="Turn on debugging")
      var debug : Boolean = false

      @Parameter(names=Array("-T","--tool"),description="The tool to use")
      var tool : String = null

      @Parameter(names=Array("-L","--intervals"),description="The intervals to extract")
      var intervals : String = null
    }

  def parse_arguments(args: Array[String]) {
    new JCommander(GAPConfig,args.toArray: _*)
    run()
  }

  def run() : Boolean = {
    // todo -- use reflection here
    if ( GAPConfig.tool == "PrintTable" ) {
      val printTable = new PrintTable(GAPConfig.debug,GAPConfig.inFiles.toList.map((x: String) => new File(x)))
      return printTable.run()
    } else if ( GAPConfig.tool == "CountReads" ) {
      val readCounter = new CountReads(GAPConfig.debug,GAPConfig.inFiles.toList.map((x:String) => new File(x)),GAPConfig.intervals)
    }

    false
  }


  def main(args:Array[String]) {
    parse_arguments(args)
  }


}

}