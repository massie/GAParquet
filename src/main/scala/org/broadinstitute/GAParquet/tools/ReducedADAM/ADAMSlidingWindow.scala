package org.broadinstitute.GAParquet.tools.ReducedADAM

import scala.collection.{SortedSet, mutable}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import org.broadinstitute.sting.gatk.walkers.compression.reducereads._
import org.broadinstitute.sting.utils.{UnvalidatingGenomeLoc, GenomeLoc}
import scala.collection.mutable.ListBuffer
import org.broadinstitute.sting.gatk.walkers.compression.reducereads.SlidingWindow.ConsensusType
import java.util.PriorityQueue
import scala.collection.JavaConversions._
import scala.collection.immutable.TreeSet
import it.unimi.dsi.fastutil.bytes.{Byte2IntArrayMap, Byte2IntMap}

/**
 * Created with IntelliJ IDEA.
 * User: chartl
 * Date: 10/3/13
 * Time: 1:53 PM
 * To change this template use File | Settings | File Templates.
 */
abstract class ADAMSlidingWindow {}
/*class ADAMSlidingWindow(_context : Int, _contig: String, _contigIndex : Int,
                        _downCvg : Int, _name: String, _altP : Double, _altProp: Double,
                        _indelProp : Double, _minBaseQual : Double, _minMapQual : Double) {

  var readsInWindow : PriorityQueue[ADAMRecord] = new PriorityQueue[ADAMRecord]()
  var windowHeader : ListBuffer[HeaderElement] = new ListBuffer[HeaderElement]()
  val contextSize : Int = _context
  val contig : String = _contig
  val contigIndex : Int = _contigIndex
  val downsampleCoverage : Int = _downCvg

  val consensusCounter : Int = 0
  val filteredDataConsensusCounter : Int = 0
  val consensusReadName : String = _name

  val MIN_ALT_PVALUE_TO_TRIGGER_VARIANT : Double = _altP
  val MIN_ALT_PROPORTION_TO_TRIGGER_VARIANT : Double = _altProp
  val MIN_INDEL_BASE_PROPORTION_TO_TRIGGER_VARIANT : Double = _indelProp
  val MIN_BASE_QUAL_TO_COUNT : Double = _minBaseQual
  val MIN_MAPPING_QUALITY : Double = _minMapQual

  val markedSites = MarkedSites

  // not sure strategy is necessary

  def stopLocation() : Int = if ( windowHeader.size == 0 ) -1 else windowHeader(windowHeader.size - 1).getLocation

  def startLocation() : Int = if ( windowHeader.size == 0 ) -1 else windowHeader(0).getLocation

  def addRecord(rec: ADAMRecord) : CompressionStash = {
    if ( rec.getMapq >= MIN_MAPPING_QUALITY ) {
      readsInWindow.add(rec)
    }
    addToHeader(rec)
    slideWindow(getUnclippedStart(rec))
  }

  def nextVariantRegion(from: Int, to: Int, variant: Seq[Boolean], closeLast: Boolean) : FinishedGenomeLoc = {
    val startOfVar : Seq[Boolean] = variant.slice(from,to).keepWhile(!_)
    if ( startOfVar.size == 0 || ! closeLast ) {
      return null
    }
    val start : Int = from + startOfVar.size + startLocation()
    val end : Int = start + variant.slice(from,to).dropWhile(!_).keepWhile(_).size - 1
    new FinishedGenomeLoc(contig,contigIndex,start,end,true)
  }

  def findVariantRegions(from: Int, to: Int, variant: Seq[Boolean], closeLast: Boolean) : CompressionStash = {
    var stash : CompressionStash = new CompressionStash
    var index : Int = from
    while ( index < to ) {
      val result : FinishedGenomeLoc = nextVariantRegion(index,to,variant,closeLast)
      regions.add(result)
      index = if ( result != null || ! result.isFinished ) to else result.getStop - startLocation() + 1
    }
    regions
  }

  def slideWindow(incomingStart: Int) : CompressionStash = {
    val windowHeaderStart = startLocation()
    var regions = new CompressionStash
    var forceClose : Boolean = true

    if ( incomingStart - contextSize > windowHeaderStart ) {
      val variantSites : Seq[Boolean] = markSites(incomingStart)
      val readHeaderIndex = incomingStart - windowHeaderStart
      val breakpoint = math.max(readHeaderIndex-contextSize-1,0)

      regions = findVariantRegions(0,breakpoint,variantSites, !forceClose)
    }
      readsInWindow = new PriorityQueue(asJavaCollection(asScalaIterator(readsInWindow).dropWhile((p: ADAMRecord) => getSoftEnd(p) < windowHeaderStart)))
  }

  object MarkedSites {
    var variantSites : Array[Boolean] = Array[Boolean](0)
    var startLoc : Int = 0

    def updateRegion(newStart: Int, regionSize: Int) : Int = {
      var lastPositionMarked : Int = regionSize
      if ( newStart >= startLoc + variantSites.size || newStart < startLoc ) {
        lastPositionMarked = 0
        variantSites = new Array[Boolean](regionSize)
      } else if ( newStart != startLoc || regionSize != variantSites.size ) {
        // copy what we can and continue
        lastPostionMarked = math.min(variantSites.size - (newStart-startLoc), regionSize)
        var tmpArray : Array[Boolean] = new Array[Boolean](regionSize)
        variantSites.copyToArray(tmpArray,newStart-startLoc,lastPositionMarked)
        variantSites = null // clear memory
        variantSites = tmpArray
      }

      startLoc = newStart
      lastPositionMarked + startLoc
    }
  }

  def markSites(stop: Int) : Seq[Boolean] = {
    val windowStart = startLocation()
    val sizeOfMarkedRegion = stop - windowStart + contextSize + 1
    val lastMarked = markedSites.updateRegion(windowStart,sizeOfMarkedRegion)-contextSize-1
    val locToProcess = math.max(windowStart,math.min(lastMarked,stop-contextSize))
    var headerElementIterator : Iterator[HeaderElement] = windowHeader.iterator
    for (i <- locToProcess to stop ) {
      if ( headerElementIterator.hasNext ) {
        val elem = headerElementIterator.next()
        if ( elem.isVariant(MIN_ALT_PVALUE_TO_TRIGGER_VARIANT,MIN_ALT_PROPORTION_TO_TRIGGER_VARIANT,MIN_INDEL_BASE_PROPORTION_TO_TRIGGER_VARIANT) ) {
          markVariantRegion(i-windowStart)
        }
      }
    }

    markedSites.variantSites.toSeq
  }

  def markVariantRegion(variantSite: Int) {
    val from: Int = if ( variantSite > contextSize ) variantSite - contextSize  else 0
    val to: Int = if ( variantSite + contextSize + 1 > markedSites.variantSites.size ) markedSites.variantSites.size - 1 else variantSite + contextSize
    markRegionAs(from,to,true)
  }

  def markRegionAs(from: Int, _to: Int, isVariant: Boolean) {
    (from to _to).foreach(x=>markedSites.variantSites(x) = isVariant)
  }

  def addToSyntheticReads(header: ListBuffer[HeaderElement], start: Int, end: Int, consensusType: SlidingWindow.ConsensusType) : List[ADAMRecord] = {
    var finishedReads : ListBuffer[ADAMRecord] = new ListBuffer[ADAMRecord](100)
    var consensus : ADAMConsensus = null
    val headerIterator : Iterator[HeaderElement] = header.iterator
    var wasInConsensus : Boolean = false
    (start to end).foreach(curPos => {
      assert(headerIterator.hasNext)
      val elem : HeaderElement = headerIterator.next()
      if ( elem.hasConsensusData(consensusType) ) {
        wasInConsensus = true
        if ( consensus == null )
          consensus = createNewConsensus(consensusType,elem.getLocation)

        genericAddBaseToConsensus(consensus,headerElement.getBaseCounts(consensusType) )
      } else {
        if ( wasInConsensus ) {
          finishedReads ++= finalizeAndAdd(consensus,consensusType)
          consensus = null
        }

        wasInConsensus = false
      }
    })

    finishedReads ++= finalizeAndAdd(consensus,consensusType)

    finishedReads.toList
  }

  def createNewConsensus(cType: ConsensusType, start: Int) : ADAMConsensus = {
    if ( cType == ConsensusType.FILTERED ) new ADAMConsensus(_contig, _contigIndex, _name, start, "STRANDLESS") else
      if ( cType == ConsensusType.POSITIVE_CONSENSUS) new ADAMConsensus(_contig,_contigIndex,_name,start,"POSITIVE") else
      new ADAMConsensus(_contig,_contigIndex,_name,start,"NEGATIVE")
  }

  def finalizeAndAdd(consensus: ADAMConsensus, cType: ConsensusType) : List[ADAMRecord] = {
    val finalized : ListBuffer[ADAMRecord] = new ListBuffer[ADAMRecord]
    var rec : ADAMRecord = null
    if ( cType == ConsensusType.FILTERED ) {
      rec = finalizeFilteredDataConsensus(consensus)
    } else {
      rec = finalizeRunningConcensus(consensus)
    }

    if ( rec != null )
      finalized += rec

    finalized
  }

  def genericAddBaseToConsensus(consensus: ADAMConsensus, counts: BaseAndQualsCounts) {
    val baseIndex: BaseIndex = counts.baseIndexWithMostProbability()
    consensus.add(baseIndex.name.charAt(0),counts.countOfBase(baseIndex),counts.averageQualsOfBase(baseIndex),counts.getRMS)
  }

  def compressVariantRegion(start: Int, stop: Int, snpPositions: List[GenomeLoc] ) : Tuple2[List[ADAMRecord],Int] = {
    val hetRefPostion : Int = if ( snpPositions != null && snpPositions.isEmpty ) -1 else findSinglePolyploidCompressiblePosition(start,stop)
    if ( hetRefPosition != -1 && matchesKnownPosition(windowHeader.get(hetRefPostion).get.getLocation,snpPositions) ) {
      return new Tuple2(createPolyploidConsensus(hetRefPosition),hetRefPostion)
    } else {
      val refStart : Int = windowHeader.get(start).get.getLocation
      val refStop : Int = windowHeader.get(stop).get.getLocation
      val toRemoveFromWindow : ListBuffer[ADAMRecord] = new ListBuffer[ADAMRecord]()
      val toEmit : ListBuffer[ADAMRecord] = new ListBuffer[ADAMRecord]()
      asScalaIterator(readsInWindow).foreach(rec => {
        if ( getSoftStart(rec) <= refStop ) {
          if ( rec.getEnd >= refStart ) {
            toEmit += rec
            removeFromHeader(windowHeader,rec)
          }
          toRemoveFromWindow += rec
        }
      })
      toRemoveFromWindow.foreach( rec => { readsInWindow.remove(rec) } )
      new Tuple2(toEmit)
    }

    // can't get here
    null
  }

  def matchesKnownPosition(targetPosition: Int, knownSnpPositions: Seq[GenomeLoc]) : Boolean = {
    val targetLoc : GenomeLoc = new UnvalidatingGenomeLoc(_contig,_contigIndex,targetPosition,targetPosition)
    knownSnpPositions == null || knownSnpPositions.contains(targetLoc)
  }

  def findSinglePolyploidCompressiblePosition(start: Int, stop :Int) : Int = {
    var hetRefPosition : Int = -1
    (start to stop).foreach(i=> {
      val nAlleles : Int = windowHeader.get(i).getNumberOfBaseAlleles(MIN_ALT_PVALUE_TO_TRIGGER_VARIANT,MIN_ALT_PROPORTION_TO_TRIGGER_VARIANT)
      if ( nAlleles > 2 || nAlleles == -1 )
        return -1
      if ( hetRefPosition != -1 )
        return -1
      hetRefPosition = i
    })
    hetRefPosition
  }

  def hasPositionWithSignificantSoftclipsOrVariant(header: Seq[HeaderElement], positionToSkp: Int) : Boolean = {
    header.foreach(elem=>{
      if ( elem.getLocation != positionToSkp &&
           elem.hasSignificantSoftclips(MIN_ALT_PVALUE_TO_TRIGGER_VARIANT,MIN_ALT_PROPORTION_TO_TRIGGER_VARIANT) &&
           elem.getNumberOfBaseAlleles(MIN_ALT_PVALUE_TO_TRIGGER_VARIANT,MIN_ALT_PROPORTION_TO_TRIGGER_VARIANT))
        return true
    })

    false
  }

  def closeVariantRegion(start: Int, stop: Int, knownSnpPositions: Seq[GenomeLoc]) : Tuple2[List[ADAMRecord],Int] = {
    val allReads : Tuple2[List[ADAMRecord],Int] = compressVariantRegion(start,stop,knownSnpPositions)
    val additionalList : List[ADAMRecord] = allReads._1 ++ addAllSyntheticReadTypes(0,allReads._2+1)
    new Tuple2(additionalList,allReads._2)
  }

  def addAllSyntheticReadGroups(start: Int, end: Int) : List[ADAMRecord] = {
    val reads : ListBuffer[ADAMRecord] = new ListBuffer[ADAMRecord]()
    reads ++= addToSyntheticReads(windowHeader,start,end,ConsensusType.POSITIVE_CONSENSUS)
    reads ++= addToSyntheticReads(windowHeader,start,end,ConsensusType.NEGATIVE_CONSENSUS)
    reads ++= addToSyntheticReads(windowHeader,start,end,ConsensusType.FILTERED)
    reads.toList
  }

  def closeVariantRegions(regions: CompressionStash, knownSnpPositions: Seq[GenomeLoc], forceClose: Boolean) : List[ADAMRecord] = {
    val allReads : ListBuffer[ADAMRecord] = new ListBuffer[ADAMRecord]()
    if ( ! regions.isEmpty ) {
      var windowHeaderStart = startLocation()
      var lastCleanedElement : HeaderElement = null
      regions.foreach( (region : GenomeLoc) => {
        if ( region.asInstanceOf[FinishedGenomeLoc].isFinished  && region.getContig.equals(_contig) &&
             region.getStart >= windowHeaderStart && region.getStop < windowHeaderStart + windowHeader.size ) {
          var stop : Int = region.getStop - windowHeaderStart
          if ( region.getStop > markedSites.startLoc + markedSites.variantSites.length - 1 )
            markSites(region.getStop)

          var closeVariantRegionResult : Tuple2[List[ADAMRecord],Int] = closeVariantRegion(start,stop,knownSnpPositions)
          allReads ++= closeVariantRegionResult._1
          if ( stop > 0 && closeVariantRegionResult._2 < stop ) {
            // update variant sites since the context size's worth of bases after the variant position are no longer "variant"
            markRegionAs(closeVariantRegionResult._2+1,stop,false)

            // if the calling emthod said it didn't care then we are okay to update the stop
            if ( ! forceClose ) {
              stop = closeVariantRegionResult._2
            } else{
              // otherwise push the stop we originally requested
              while ( closeVariantRegionResult._2 < stop ) {
                // clean up used header elements
                windowHeader = windowHeader.drop(closeVariantRegionResult._2)
                stop -= closeVariantRegionResult._2 + 1
                closeVariantRegionResult = closeVariantRegion(0,stop,knownSnpPositions)
                allReads ++= closeVariantRegionResult._1
              }
            }
          }

          if ( stop >= 0 ) {
            windowHeader = windowHeader.drop(stop-1) // todo -- these drops don't work
          }

          lastCleanedElement = windowHeader(0)
          windowHeader = windowHeader.drop(1)
        }
      })
    }
  }

  def close(knownSnpPositions: Seq[GenomeLoc]) : Tuple2[List[ADAMRecord],CompressionStash] = {
    var finalizedReads : SortedSet[OrderedADAMRecord] = new TreeSet[OrderedADAMRecord]()
    var regions: CompressionStash = new CompressionStash
    if ( ! windowHeader.isEmpty ) {
      markSites(getStopLocation(windowHeader)+1)
      regions =findVariantRegions(0,windowHeader.size,markedSites.variantSites,true)
      finalizedReads = new TreeSet[OrderedADAMRecord](closeVariantRegions(regions,knownSnpPositions,true).map(r => new OrderedADAMRecord(r)))
      if ( ! windowHeader.isEmpty )
        finalizedReads ++= new TreeSet[OrderedADAMRecord](addAllSyntheticReadGroups(0,windowHeader.size).map(r => new OrderedADAMRecord(r)))
    }

    new Tuple2(finalizedReads.toList,regions)
  }

  def finalizeRunningConsensus(runningConsensus: ADAMConsensus) : ADAMRecord = {
    var finalized : ADAMRecord = null
    if ( runningConsensus != null ) {
      if ( runningConsensus.size > 0 ) {
        finalized = runningConsensus.close
      } else {
        consensusCounter -= 1
      }
    }

    finalized
  }

  def finalizeFilteredDataConsensus(runningConsensus: ADAMConsensus) : ADAMRecord = {
    var finalized : ADAMRecord = null
    if ( runningConsensus != null ) {
      if ( runningConsensus.size > 0 ) {
        finalized = runningConsensus.close
      } else {
        filteredDataConsensusCounter -= 1
      }
    }
    finalized
  }

  def createPolyploidConsensus(hetRefPosition: Int) : List[ADAMRecord] = {
    val headersPosStrand : Array[Tuple2[List[HeaderElement],List[ADAMRecord]]] = new Array[Tuple2[List[HeaderElement],List[ADAMRecord]]](2)
    val headersNegStrand : Array[Tuple2[List[HeaderElement],List[ADAMRecord]]] = new Array[Tuple2[List[HeaderElement],List[ADAMRecord]]](2)
    val globalHetRefPosition = windowHeader.get(hetRefPosition).getLocation
    // initialize the mapping from the allele to the header
    val alleleHeaderMap : Byte2IntMap = new Byte2IntArrayMap(2)
    alleleHeaderMap.defaultReturnValue(-1)
    windowHeader.get(hetRefPosition).getAlleles(MIN_ALT_PVALUE_TO_TRIGGER_VARIANT,MIN_ALT_PROPORTION_TO_TRIGGER_VARIANT).foreach( (allele:BaseIndex) => {
      val currentIndex : Int = alleleHeaderMap.size()
      if ( curentIndex.size > 1 )
        throw new IllegalStateException("There are more than 2 alleels present when creating a diploid consensus")

    })

  }
}*/
