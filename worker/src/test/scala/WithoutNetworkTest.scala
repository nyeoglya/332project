package worker

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.rogach.scallop._
import Main._
import common._

import zio.test._
import scala.language.postfixOps

// <1st test>
// gensort -a -b0 1000 input1.txt
// gensort -a -b1000 1000 input2.txt
// gensort -a -b2000 1000 input3.txt
// gensort -a -b3000 1000 input4.txt
//
// valsort mergedFile1
// valsort mergedFile2
// type mergedFile1 mergedFile2 > mergedFile
// valsort mergedFile

@RunWith(classOf[JUnitRunner])
class WithoutNetworkTest extends FunSuite {
  test("dummy test") {
    assertTrue(true)
  }

  test("Main Config") {
    val args = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    Main.main(args)
    assertTrue(config.masterAddress.toOption.get == "141.223.91.80:30040")
    assertTrue(config.inputDirectories.toOption.get == List("src/test/withoutNetworkTestFiles/worker1/input"))
    assertTrue(config.outputDirectory.toOption.get == "src/test/withoutNetworkTestFiles/worker1/output")
  }

  test("overall correctness") {

    val startTime = System.nanoTime()

    Mode.testMode = "WithoutNetworkTest"
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val args2 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker2/input -O src/test/withoutNetworkTestFiles/worker2/output".split(' ')

    Mode.machineNumber = 1
    Main.main(args1)
    val sample1 = Main.sampleStream

    Mode.machineNumber = 2
    Main.main(args2)
    val sample2 = Main.sampleStream

    val sortedSample = (sample1 ++ sample2).sortBy(entity => entity.head)
    val pivots = List(sortedSample(sortedSample.length / 2))

    Mode.machineNumber = 1
    Mode.pivotList = pivots
    Main.main(args1)
    val from1 = Main.beforeShuffleStreams

    Mode.machineNumber = 2
    Mode.pivotList = pivots
    Main.main(args2)
    val from2 = Main.beforeShuffleStreams

    Mode.machineNumber = 1
    Mode.shuffledStreams = List(from1(0), from2(0))
    Main.main(args1)
    val result1 = Main.mergedFilePath

    Mode.machineNumber = 2
    Mode.shuffledStreams = List(from1(1), from2(1))
    Main.main(args2)
    val result2 = Main.mergedFilePath

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e6
    // this is for comparing before and after parallelization
    // to check whether parallelization really works
    // [1st] test time : 112978.4586 ms
    // [2nd] test time : 109899.5734 ms
    println(s"test time : $duration ms")
  }
}
