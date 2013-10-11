package org.broadinstitute.GAParquet.tools.ReducedADAM

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import net.sf.samtools.{CigarOperator, CigarElement, Cigar}

/**
 * Created with IntelliJ IDEA.
 * User: chartl
 * Date: 10/3/13
 * Time: 4:48 PM
 * To change this template use File | Settings | File Templates.
 */
class ADAMConsensus(val _contig: String, val _contigIndex: Int, val _readName: String, var _start: Int, val _strandType: String) {

  object StrandType extends Enumeration {
    type StrandType = Value
    val POSITIVE,NEGATIVE,STRANDLESS  = Value
  }

  class BaseInfo(_base: Char, _count: Int, _qual: Int) {
    val base : Char = _base
    var count : Int = _count
    var qual : Byte = _qual.toByte
  }

  val strType = StrandType.withName(_strandType)

  val initCapcacity = 10000

  var baseCountsQuals : ListBuffer[BaseInfo] = new ListBuffer[BaseInfo]()

  var mappingQuality : Double = 0

  def add(base: Char, count: Int, qual: Byte, mapQ: Double) : Unit = {
    baseCountsQuals += new BaseInfo(base,count,qual)
    mappingQuality += mapQ
  }

  def size : Int = baseCountsQuals.size

  def getBase(offset: Int) : Char = baseCountsQuals.get(offset).base

  def convertBaseQualities() : Array[Byte] = {
    baseCountsQuals.filter( u => ! u.base.equals('D') ).map(t => t.qual).toArray
  }

  def convertReadBases() : CharSequence = new String(baseCountsQuals.filter(u => ! u.base.equals('D')).map(t => t.base).toArray)

  def convertBaseCounts : Array[Int] = baseCountsQuals.filter(u => ! u.base.equals('D')).map(t => t.count).toArray

  def close : ADAMRecord = {
    var consensusRecord : ADAMRecord = new ADAMRecord()
    consensusRecord.setReferenceId(_contigIndex)
    consensusRecord.setReferenceName(_contig)
    consensusRecord.setReadPaired(false)
    consensusRecord.setReadMapped(true)
    consensusRecord.setCigar(buildCigar)
    if ( strType != StrandType.STRANDLESS ) {
      consensusRecord.setReadNegativeStrand(strType == StrandType.NEGATIVE)
    }
    consensusRecord.setStart(_start)
    consensusRecord.setReadName(_readName)
    consensusRecord.setQual(convertBaseQualities().toString.asInstanceOf[CharSequence])
    consensusRecord.setSequence(convertReadBases())
    consensusRecord.setDuplicateRead(false)
    //consensusRecord.setReducedCoverageCounts(convertBaseCounts())

    consensusRecord
  }

  def buildCigar : CharSequence = {
    var elements : ListBuffer[CigarElement] = new ListBuffer[CigarElement]()
    var length : Int = 0
    var currentOperator : CigarOperator = null
    baseCountsQuals.foreach( baseInfo => {
      var op : CigarOperator = null
      if ( baseInfo.base == 'D' ) { op = CigarOperator.DELETION }
      else if ( baseInfo.base == 'I' ) { throw new IllegalArgumentException("Cannot place insertion into an ADAMConsensus") }
      else { op = CigarOperator.MATCH_OR_MISMATCH }
      if ( currentOperator == null ) {
        if ( op == CigarOperator.DELETION ) // can't start with deletion
          _start += 1
        else
          currentOperator = op
      } else if ( currentOperator != op ) {
        // new operator, close old and dstart afresh
        elements += new CigarElement(length,currentOperator)
        currentOperator = op
        length = 0
      }
      if ( currentOperator != null )
        length += 1
    })
    if ( length > 0 && currentOperator != CigarOperator.DELETION )
      elements += new CigarElement(length,currentOperator)

    (new Cigar(bufferAsJavaList(elements))).toString.asInstanceOf[CharSequence]
  }
}
