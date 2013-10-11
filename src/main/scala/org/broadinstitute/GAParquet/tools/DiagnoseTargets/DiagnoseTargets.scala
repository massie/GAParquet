package org.broadinstitute.GAParquet.tools.DiagnoseTargets

import java.io.File
import org.broadinstitute.GAParquet.tools.Crusher
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import spark.RDD
import scala.collection.JavaConversions._
import org.broadinstitute.sting.utils.text.XReadLines
import org.broadinstitute.GAParquet.utils.{GADAMRecord, HasInterval, IntervalSet, Interval}
import scala.collection.immutable.HashMap

/**
 * Created with IntelliJ IDEA.
 * User: chartl
 * Date: 10/7/13
 * Time: 3:44 PM
 * To change this template use File | Settings | File Templates.
 */
class DiagnoseTargets(d: Boolean, files: Seq[File], val intervals: String) extends Crusher(files) {

  val MIN_BASE_QUAL : Int = 20
  val MIN_MAP_QUAL : Int = 20
  val MIN_CVG : Int = 5
  val MAX_CVG : Int = Integer.MAX_VALUE/2
  val MAX_INSERT : Int = 500
  val VOTE_PCT_THRESHOLD = 0.5 // for multi sample
  val BAD_MATE_THRESH = 0.5
  val CVG_STAT_THRESHOLD = 0.2
  val EXCESSIVE_CVG_THRESHOLD = 0.2
  val QUAL_STAT_THRESH = 0.5


  override def name() = "DiagnoseTargets"

  override def server() : String = "local"

  class IntervalInclusionTransformer(intervalFile: File) extends Iterable[Interval] {
    // assume the intervals are sorted (for reference IDs)
    val lines : Iterator[String] = asScalaIterator(new XReadLines(intervalFile))
    val intervalMap : Map[String,IntervalSet] = lines.map(parseInterval).foldLeft[Map[String,List[Interval]]](new HashMap[String,List[Interval]])(mergeInterval).mapValues(new IntervalSet(_))

    def parseInterval(intString: String) : Interval = {
      // for now assume bed format (tab delimited)
      val splitLine : Array[String] = intString.split("\t")
      val contig: String = splitLine(0)
      val start: Long = splitLine(1).toLong
      val end: Long = if ( splitLine.size > 2 ) splitLine(2).toLong else start
      val annotations : Map[String,String] = if ( splitLine.size > 3 ) ( splitLine(3).split(";").foldLeft[Map[String,String]](new HashMap[String,String]())((x,y) => {
        val kv = y.split("=")
        x + (kv(1)->kv(2))
        x
      }) ) else null

      new Interval(contig,start,end,annotations)
    }

    def mergeInterval(x: Map[String,List[Interval]], y: Interval) : Map[String,List[Interval]] = {
      if ( ! x.contains(y.getReferenceName) ) {
        x + (y.getReferenceName -> List(y))
      } else {
        x + (y.getReferenceName -> (x(y.getReferenceName) :+ y))
      }
    }

    def getIntervals(int: HasInterval) : Seq[Interval] = intervalMap(int.getReferenceName).getOverlapping(int)

    def getFirstInterval(int: HasInterval) : Interval = {
      val overlaps : Seq[Interval] = intervalMap(int.getReferenceName).getOverlapping(int)
      if ( overlaps.size > 0 ) overlaps(0) else null
    }

    def iterator = intervalMap.map(t => t._2.iterator).reduceLeft((x,y) => x ++ y)
  }

  def mqFilter(r: ADAMRecord) : Boolean = r.getMapq >= MIN_MAP_QUAL
  def insFilter(r: ADAMRecord) : Boolean = r.getReferenceName.equals(r.getMateReference) && ((math.abs(r.getStart-r.getMateAlignmentStart)-2*(r.getEnd-r.getStart)) <= MAX_INSERT)

  class IntervalDiagnosis(val interval: Interval, records: Seq[ADAMRecord]) {
    // this is all about coverage
    val intervalCoverage : Array[Int] = records.filter(mqFilter).filter(insFilter).foldLeft(new Array[Int](interval.size.toInt))((cvg : Array[Int], rec: ADAMRecord) => {
      // really? CharSequence is not iterable???
      val overlapingSubSeq : Seq[Char] = rec.getQual.subSequence(math.max(interval.getStart.toInt-rec.getStart.toInt,0),rec.getEnd.toInt-rec.getStart.toInt-math.max(0,rec.getEnd.toInt-interval.getEnd.toInt)).asInstanceOf[Seq[Char]]
      val start = math.max(0,rec.getStart - interval.getStart)
      overlapingSubSeq.map(_.toByte <= MIN_BASE_QUAL).zipWithIndex.foreach( (q: (Boolean,Int)) => {cvg(q._2) += ( if ( q._1 ) 1 else 0 )})
      cvg
    })
    val n_below_coverage = intervalCoverage.count(_ <= MIN_CVG)
    val n_above_max_coverage = intervalCoverage.count(_ >= MAX_CVG)
    val n_bad_mate = records.count(insFilter)
    val n_bad_mapping = records.count(mqFilter)
    val n_no_cvg = intervalCoverage.count(_ == 0)
    val n_reads = records.size
    val n_bases = intervalCoverage.size

    def getStatus : List[String] = {
      var status : List[String] = Nil
      if ( n_below_coverage >= n_bases*CVG_STAT_THRESHOLD) {
        status :+= "LOW_COVERAGE"
      }
      if ( n_no_cvg >= n_bases*CVG_STAT_THRESHOLD) {
        status :+= "COVERAGE_GAPS"
      }
      if ( n_bad_mate >= n_reads*BAD_MATE_THRESH ) {
        status :+= "BAD_MATE"
      }
      if ( n_above_max_coverage >= n_bases*EXCESSIVE_CVG_THRESHOLD ) {
        status :+= "EXCESSIVE_COVERAGE"
      }
      if ( n_bad_mapping >= n_reads*BAD_MATE_THRESH ) {
        status :+= "POOR_MAPPING_QUALITY"
      }
      if ( n_no_cvg == n_bases  ) {
        status :+= "NO_COVERAGE"
      }
      if ( status.size > 0 ) status else List("PASS")
    }
  }

  def analyze(rdd: RDD[(Void,ADAMRecord)]) : Unit = {
    val intervalTransformer : IntervalInclusionTransformer = new IntervalInclusionTransformer(new File(intervals))
    // some ugliness here, no way to nicely drop the Void keys in Spark
    val groupedByIntervals : RDD[(Interval,Seq[(Void,ADAMRecord)])] = rdd.groupBy[Interval]((x: (Void,ADAMRecord)) => { intervalTransformer.getFirstInterval(new GADAMRecord(x._2)) })
    // todo -- partition by samples
    val diagnosed = groupedByIntervals.filter(x => x._1 != null ).map( (x: (Interval,Seq[(Void,ADAMRecord)])) => (x._1,new IntervalDiagnosis(x._1,x._2.map(_._2))))
    val status : RDD[(Interval,List[String])] = diagnosed.map( (x: (Interval, IntervalDiagnosis)) => {(x._1,x._2.getStatus)})
    status.foreach(z => println(z._1.toString + "\t" + z._2.reduceLeft(_ + ", " + _)))
  }
}
