package worker

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import Main._
import common._
import java.io.{File, PrintWriter}
import scala.io.Source
import zio._
import zio.stream._
import java.nio.file.Paths

@RunWith(classOf[JUnitRunner])
class WorkerSuite extends FunSuite {

  def isDataSorted(data : List[Entity]) : Boolean = {
    def isDataSortedAux(data : List[Entity], prev : String) : Boolean = {
      data match {
        case Nil => true
        case e::nextData =>
          if(e.head > prev) isDataSortedAux(nextData, e.head)
          else false
      }
    }
    isDataSortedAux(data, "00")
  }

  val worker1data : List[List[Entity]] =
    List(readTestFile("testFile1.txt"), readTestFile("testFile2.txt"))
  val worker1result : List[Entity] =
    List(readTestFile("resultFile1.txt"), readTestFile("resultFile2.txt")).flatten
  val worker2data : List[List[Entity]] =
    List(readTestFile("testFile3.txt"), readTestFile("testFile4.txt"))
  val worker2result : List[Entity] =
    List(readTestFile("resultFile3.txt"), readTestFile("resultFile4.txt")).flatten

  val offset = 3
  val M = 2
  val N = 2

  val sortedFilePaths =
    List(sortSmallFile("testFile1.txt"), sortSmallFile("testFile2.txt"), sortSmallFile("testFile3.txt"), sortSmallFile("testFile4.txt"))
  lazy val sortedFileDatas =
    sortedFilePaths.map(path => readTestFile(path))

  val sampledFilePaths =
    sortedFilePaths.map(path => produceSampleFile(path, offset))
  val sampledFileDatas =
    sampledFilePaths.map(path => readTestFile(path))

  val w1SampleStream = sampleFilesToSampleStream(sampledFilePaths.take(M))

  val w2SampleStream = sampleFilesToSampleStream(sampledFilePaths.drop(M))
  val partitionStreams =
    sortedFilePaths.map(path => splitFileIntoPartitionStreams(path, List("21")))

  val w1Tow1 =
    mergeBeforeShuffle(List(partitionStreams(0)(0), partitionStreams(1)(0)))
  val w1Tow2 =
    mergeBeforeShuffle(List(partitionStreams(0)(1), partitionStreams(1)(1)))
  val w2Tow1 =
    mergeBeforeShuffle(List(partitionStreams(2)(0), partitionStreams(3)(0)))
  val w2Tow2 =
    mergeBeforeShuffle(List(partitionStreams(2)(1), partitionStreams(3)(1)))

  test("dummy test") {
    assert(true)
  }

  test("sortSmallFile test : sorted correctly ") {
    assert(sortedFileDatas.forall(entitys => isDataSorted(entitys)))
  }

  test("produceSampleFile test : subset ") {
    assert(sampledFileDatas.last.forall(entity => sortedFileDatas.last.contains(entity)))
  }

  test("produceSampleFile test : sorted ") {
    assert(sampledFileDatas.forall(entitys => isDataSorted(entitys)))
  }

  test("produceSampleFile test : length ") {
    assert(sampledFileDatas.last.length == (sortedFileDatas.last.length + offset - 1) / offset)
  }

  test("sampleFilesToSampleStream test : length ") {
    assert(w1SampleStream.length == sampledFileDatas(0).length + sampledFileDatas(1).length)
  }

  test("splitFileIntoPartitionStreams test : N stream ") {
    assert(partitionStreams.head.length == N)
  }

  test("splitFileIntoPartitionStreams test : length ") {
    val partitionedLenSum = Unsafe.unsafe { implicit unsafe =>
      partitionStreams.head.map(st => Runtime.default.unsafe.run(st.runCount).getOrThrow()).sum
    }
    assert(partitionedLenSum == sortedFileDatas.head.length)
  }

  test("splitFileIntoPartitionStreams test : sort ") {
    val partitionedDataList = Unsafe.unsafe { implicit unsafe =>
      partitionStreams.head.map(st => Runtime.default.unsafe.run(st.runCollect.map(_.toList)).getOrThrow())
    }
    val partitionedDataList2 = Unsafe.unsafe { implicit unsafe =>
      partitionStreams(1).map(st => Runtime.default.unsafe.run(st.runCollect.map(_.toList)).getOrThrow())
    }
    val partitionedDataList3 = Unsafe.unsafe { implicit unsafe =>
      partitionStreams(2).map(st => Runtime.default.unsafe.run(st.runCollect.map(_.toList)).getOrThrow())
    }
    val partitionedDataList4 = Unsafe.unsafe { implicit unsafe =>
      partitionStreams(3).map(st => Runtime.default.unsafe.run(st.runCollect.map(_.toList)).getOrThrow())
    }
    assert(partitionedDataList.forall(entitys => isDataSorted(entitys)))
    assert(partitionedDataList2.forall(entitys => isDataSorted(entitys)))
    assert(partitionedDataList3.forall(entitys => isDataSorted(entitys)))
    assert(partitionedDataList4.forall(entitys => isDataSorted(entitys)))
  }

  test("mergeBeforeShuffle test : sorted ") {
    val w1Tow2Data = Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(w1Tow2.runCollect.map(_.toList)).getOrThrow()
    }
    assert(isDataSorted(w1Tow2Data))
  }

  test("mergeBeforeShuffle test : length ") {
    val outputLen : Long = Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(w1Tow2.runCount).getOrThrow()
    }
    val partitionLen1 = Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(partitionStreams(0)(1).runCount).getOrThrow()
    }
    val partitionLen2 = Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(partitionStreams(1)(1).runCount).getOrThrow()
    }
    assert(outputLen == partitionLen1 + partitionLen2)
  }

  test("mergeAfterShuffle test : whole correctness ") {
    val w1MergedFilePath = mergeAfterShuffle(List(w1Tow1, w2Tow1))
    assert(readTestFile(w1MergedFilePath) == worker1result)
    val w2MergedFilePath = mergeAfterShuffle(List(w1Tow2, w2Tow2))
    assert(readTestFile(w2MergedFilePath) == worker2result)
  }

}
