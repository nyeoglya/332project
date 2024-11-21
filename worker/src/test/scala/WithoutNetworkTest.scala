package worker

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.rogach.scallop._
import Main._
import common._
import proto.common.{Entity, Pivots}
import zio.{Ref, Runtime, Unsafe, ZLayer}
import zio.test._

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

  test("splitFileIntoPartitionStreams test : N stream ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val partitionedStreams = worker1.splitFileIntoPartitionStreams(worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
    assert(partitionedStreams.length == 2)
  }

  test("splitFileIntoPartitionStreams test : length ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val partitionedStreams = worker1.splitFileIntoPartitionStreams(worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
    val partitionedLengthSum = Unsafe.unsafe { implicit unsafe =>
      partitionedStreams.map(st => Runtime.default.unsafe.run(st.runCount).getOrThrow()).sum
    }
    assert(partitionedLengthSum == worker1.readFile(worker1.sortedSmallFilePaths.head).length)
  }

  test("splitFileIntoPartitionStreams test : sort ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val partitionedStreams = worker1.splitFileIntoPartitionStreams(worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
    val partitionedDatas = Unsafe.unsafe { implicit unsafe =>
      partitionedStreams.map(st => Runtime.default.unsafe.run(st.runCollect.map(_.toList)).getOrThrow())
    }
    assert(partitionedDatas.forall(entitys => isDataSorted(entitys)))
  }

  test("mergeStreams test : length ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val partitionStreams = worker1.sortedSmallFilePaths.map(path => worker1.splitFileIntoPartitionStreams(path, List("FP]Wi|7_W9")))
    val toWorkerStreams = for {
      n <- (0 to 1).toList
      toN = partitionStreams.map(_(n))
    } yield toN
    val toWorker1 = toWorkerStreams.head
    val toWorker1LengthSum = Unsafe.unsafe { implicit unsafe =>
      toWorker1.map(st => Runtime.default.unsafe.run(st.runCount).getOrThrow()).sum
    }
    val mergedStream1 = worker1.mergeStreams(toWorkerStreams.head)
    val mergedStream1Length = Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(mergedStream1.runCount).getOrThrow()
    }
    assert(mergedStream1Length == toWorker1LengthSum)
  }

  test("mergeStreams test : sorted ") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val mergedStreams = worker1.getDataStream(new Pivots(List("FP]Wi|7_W9")))
    val mergedDatas = Unsafe.unsafe { implicit unsafe =>
      mergedStreams.map(st => Runtime.default.unsafe.run(st.runCollect.map(_.toList)).getOrThrow())
    }
    assert(mergedDatas.forall(entities => isDataSorted(entities)))
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

    val from1 = worker1.getDataStream(new Pivots(pivots))
    val from2 = worker2.getDataStream(new Pivots(pivots))
    println("from done")
    val result1 = worker1.sortStreams(List(from1(0), from2(0)))
    println("result1 done")
    val result2 = worker2.sortStreams(List(from1(1), from2(1)))
    println("result2 done")
    val resultFilePath1 = worker1.saveEntities(1, result1)
    println("result1 saved")
    val resultFilePath2 = worker2.saveEntities(2, result2)
    println("result2 saved")
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e6
    // this is for comparing before and after parallelization
    // to check whether parallelization really works
    // [1st] test time : 111675.2746 ms
    // [2nd] test time : 116117.8385 ms
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

    val offset = (worker1.getFileSize() + worker2.getFileSize()) / 1000

    val sample1 = worker1.getSampleList(offset)
    val sample2 = worker2.getSampleList(offset)
    val sortedSample = (sample1 ++ sample2).sortBy(entity => entity.head)
    val pivots = List(sortedSample(sortedSample.length / 2))

    val from1 = worker1.getDataStream(new Pivots(pivots))
    val from2 = worker2.getDataStream(new Pivots(pivots))
    println("from done")
    val result1 = worker1.sortStreams(List(from1(0), from2(0)))
    println("result1 done")
    val result2 = worker2.sortStreams(List(from1(1), from2(1)))
    println("result2 done")
    val len1 = Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(result1.runCount).getOrThrow()
    }
    println(len1)
    val len2 = Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(result2.runCount).getOrThrow()
    }
    println(len2)
    val resultFilePath1 = worker1.saveEntities(1, result1)
    println("result1 saved")
    val resultFilePath2 = worker2.saveEntities(2, result2)
    println("result2 saved")
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e6
    // this is for comparing before and after parallelization
    // to check whether parallelization really works
    // [1st] test time : 111675.2746 ms
    // [2nd] test time : 116117.8385 ms
    println(s"test time : $duration ms")

    val resultEntities1 = worker1.readFile(resultFilePath1)
    val resultEntities2 = worker2.readFile(resultFilePath2)
    assert(isDataSorted(resultEntities1))
    assert(isDataSorted(resultEntities2))
    assert(isDataSorted(resultEntities1 ++ resultEntities2))
  }
}
