package org.broadinstitute.GAParquet.tools.ReducedADAM

import java.io.File
import org.broadinstitute.GAParquet.tools.Crusher

/**
 * Created with IntelliJ IDEA.
 * User: chartl
 * Date: 10/3/13
 * Time: 1:52 PM
 * To change this template use File | Settings | File Templates.
 */
abstract class ReduceADAM(d: Boolean, files: Seq[File], intervals: String) extends Crusher(files) {

}
