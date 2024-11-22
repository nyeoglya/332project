package worker

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.rogach.scallop._
import Main._
import common._
import proto.common.{Entity, Pivots}
import zio.stream.{ZPipeline, ZStream}
import zio.{Ref, Runtime, Unsafe, ZLayer}
import zio.test._

import java.io.File
import scala.collection.mutable.PriorityQueue
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

  def isDataSorted(data : List[Entity]) : Boolean = {
    def isDataSortedAux(data : List[Entity], prev : String) : Boolean = {
      data match {
        case Nil => true
        case e::nextData =>
          if(e.head > prev) isDataSortedAux(nextData, e.head)
          else false
      }
    }
    isDataSortedAux(data, "")
  }

  test("dummy test") {
    assert(true)
  }

  test("sortSmallFile test : sorted correctly ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val sortedDatas = worker1.sortedSmallFilePaths.map(path => worker1.readFile(path))
    assert(sortedDatas.forall(entities => isDataSorted(entities)))
  }

  test("produceSampleFile test : subset ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val originalData = worker1.readFile("src/test/withoutNetworkTestFiles/worker1/input/input1.txt")
    val sampleFilePath = worker1.produceSampleFile("src/test/withoutNetworkTestFiles/worker1/input/input1.txt", 100)
    val sampleData = worker1.readFile(sampleFilePath)
    assert(sampleData.forall(entity => originalData.contains(entity)))
  }

  test("produceSampleFile test : sorted ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val sampleFilePath = worker1.produceSampleFile(worker1.sortedSmallFilePaths.head, 100)
    val sampleData = worker1.readFile(sampleFilePath)
    assert(isDataSorted(sampleData))
  }

  test("produceSampleFile test : length ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val originalData = worker1.readFile("src/test/withoutNetworkTestFiles/worker1/input/input1.txt")
    val sampleFilePath = worker1.produceSampleFile("src/test/withoutNetworkTestFiles/worker1/input/input1.txt", 100)
    val sampleData = worker1.readFile(sampleFilePath)
    assert(sampleData.length == (originalData.length + 100 - 1) / 100)
  }

  test("sampleFilesToSampleList test : length ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val sampleFilePaths = worker1.sortedSmallFilePaths.map(path => worker1.produceSampleFile(path, 100))
    val sampleDatas = sampleFilePaths.map(path => worker1.readFile(path))
    val sampleList = worker1.sampleFilesToSampleList(sampleFilePaths)
    assert(sampleList.length == sampleDatas.map(_.length).sum)
  }

  test("makePartitionedFiles test : N Files ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val partitionedFiles = worker1.makePartitionedFiles(worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
    assert(partitionedFiles.length == 2)
  }

  test("makePartitionedFiles test : length ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val partitionedFiles = worker1.makePartitionedFiles(worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
    assert(partitionedFiles.map(path => worker1.readFile(path).length).sum == worker1.readFile(worker1.sortedSmallFilePaths.head).length)
  }

  test("makePartitionedFiles test : sort ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val partitionedFiles = worker1.makePartitionedFiles(worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
    val partitionedDatas = partitionedFiles.map(path => worker1.readFile(path))
    assert(partitionedDatas.forall(entities => isDataSorted(entities)))
  }

  test("mergeTwoFile test: length ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/small_input -O src/test/withoutNetworkTestFiles/worker1/small_output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val originalLength = worker1.sortedSmallFilePaths.map(path => worker1.readFile(path).length).sum
    val mergedFilePath = worker1.mergeTwoFile(0, worker1.sortedSmallFilePaths.head, worker1.sortedSmallFilePaths(1))
    val data = worker1.readFile(mergedFilePath)
    assert(data.length == originalLength)
  }

  test("mergeTwoFile test: sort ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/small_input -O src/test/withoutNetworkTestFiles/worker1/small_output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val mergedFilePath = worker1.mergeTwoFile(0, worker1.sortedSmallFilePaths.head, worker1.sortedSmallFilePaths(1))
    val data = worker1.readFile(mergedFilePath)
    assert(isDataSorted(data))
  }

  test("mergeWrite test : length ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val originalLength = worker1.sortedSmallFilePaths.map(path => worker1.readFile(path).length).sum
    val mergedFile = worker1.mergeWrite(1, worker1.sortedSmallFilePaths)
    val mergedLength = worker1.readFile(mergedFile).length
    assert(originalLength == mergedLength)
  }

  test("mergeStreams test : sorted ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val mergedFile = worker1.mergeWrite(1, worker1.sortedSmallFilePaths)
    val mergedData = worker1.readFile(mergedFile)
    assert(isDataSorted(mergedData))
  }

  test("overall correctness : total 4000 entities ") {

    val startTime = System.nanoTime()

    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val args2 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker2/input -O src/test/withoutNetworkTestFiles/worker2/output".split(' ')

    val worker1 = new WorkerLogic(new worker.Config(args1))
    val worker2 = new WorkerLogic(new worker.Config(args2))

    val offset = (worker1.getFileSize() + worker2.getFileSize()) / 1000

    val sample1 = worker1.getSampleList(offset)
    val sample2 = worker2.getSampleList(offset)

    val sortedSample = (sample1 ++ sample2).sortBy(entity => entity.head)
    val pivots = List(sortedSample(sortedSample.length / 2))

    val from1 = worker1.getToWorkerNFilePaths(new Pivots(pivots))
    val from2 = worker2.getToWorkerNFilePaths(new Pivots(pivots))

    val resultFilePath1 = worker1.mergeWrite(1,List(from1(0), from2(0)).flatten)
    val resultFilePath2 = worker2.mergeWrite(2,List(from1(1), from2(1)).flatten)

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e6
    // this is for comparing before and after parallelization
    // to check whether parallelization really works
    // [1st] test time : 111675.2746 ms
    // [2nd] test time : 187.5186 ms
    println(s"test time : $duration ms")

    val resultEntities1 = worker1.readFile(resultFilePath1)
    val resultEntities2 = worker2.readFile(resultFilePath2)
    assert(isDataSorted(resultEntities1))
    assert(isDataSorted(resultEntities2))
    assert(isDataSorted(resultEntities1 ++ resultEntities2))
  }

  test("overall correctness : total 40000 entities ") {

    val startTime = System.nanoTime()

    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/big_input -O src/test/withoutNetworkTestFiles/worker1/big_output".split(' ')
    val args2 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker2/big_input -O src/test/withoutNetworkTestFiles/worker2/big_output".split(' ')

    val worker1 = new WorkerLogic(new worker.Config(args1))
    val worker2 = new WorkerLogic(new worker.Config(args2))

    val offset = (worker1.getFileSize() + worker2.getFileSize()) / 10000

    val sample1 = worker1.getSampleList(offset)
    val sample2 = worker2.getSampleList(offset)

    val sortedSample = (sample1 ++ sample2).sortBy(entity => entity.head)
    val pivots = List(sortedSample(sortedSample.length / 2))

    val from1 = worker1.getToWorkerNFilePaths(new Pivots(pivots))
    val from2 = worker2.getToWorkerNFilePaths(new Pivots(pivots))

    val resultFilePath1 = worker1.mergeWrite(1,List(from1(0), from2(0)).flatten)
    val resultFilePath2 = worker2.mergeWrite(2,List(from1(1), from2(1)).flatten)

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e6
    // this is for comparing before and after parallelization
    // to check whether parallelization really works
    // [1st] test time : 1032829.9738 ms
    // [2nd] test time : 500.1778 ms
    println(s"test time : $duration ms")

    val resultEntities1 = worker1.readFile(resultFilePath1)
    val resultEntities2 = worker2.readFile(resultFilePath2)
    assert(isDataSorted(resultEntities1))
    assert(isDataSorted(resultEntities2))
    assert(isDataSorted(resultEntities1 ++ resultEntities2))
  }
}
