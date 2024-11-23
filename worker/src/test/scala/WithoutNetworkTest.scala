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
import java.nio.file.{Files, Paths}
import scala.collection.mutable.PriorityQueue
import scala.language.postfixOps
import sys.process._

// <test example>
// gensort -a -b0 1000 input1.txt
// gensort -a -b1000 1000 input2.txt
// gensort -a -b2000 1000 input3.txt
// gensort -a -b3000 1000 input4.txt
//
// valsort mergedFile1
// valsort mergedFile2
// type mergedFile1 mergedFile2 > mergedFile
// valsort mergedFile


/** Prerequisites
 * locate gensort.exe, gpl-2.0.txt, valsort.exe, zlib1.dll on withoutNetworkTestFiles folder
 * make worker1 ~ worker10 folder on withoutNetworkTestFiles folder
 *
 */
@RunWith(classOf[JUnitRunner])
class WithoutNetworkTest extends FunSuite {

  def createArgs(workerNum: Int, testName: String): List[Array[String]] = {
    (1 to workerNum)
      .toList.map{num =>
        val arg = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker" +
          num.toString + "/" + testName + "_input -O src/test/withoutNetworkTestFiles/worker" +
          num.toString + "/" + testName + "_output"
        arg.split(' ')
      }
  }

  /** automatically generate gensort
   * this function works only in windows by now
   *
   * @param workerNum
   * @param size
   * @param fileNum
   * @param testName
   */
  def createFiles(workerNum: Int, fileNum: Int, fileSize: Int, testName: String): Unit = {
    (1 to workerNum)
      .toList.foreach { num =>
        val inputFolder = new File("src/test/withoutNetworkTestFiles/worker" + num.toString + "/" + testName + "_input")
        val outputFolder = new File("src/test/withoutNetworkTestFiles/worker" + num.toString + "/" + testName + "_output")
        val inputFolderCmd =
          if(inputFolder.exists() && inputFolder.isDirectory)
            "rmdir /s /q " + testName + "_input"
          else "cd ."
        val outputFolderCmd =
          if(outputFolder.exists() && outputFolder.isDirectory)
            "rmdir /s /q " + testName + "_output"
          else "cd ."
        val cmd =
          "cmd /c " +
            "cd src/test/withoutNetworkTestFiles/worker" + num.toString + " && " +
            inputFolderCmd + " && " + outputFolderCmd + " && " +
            "mkdir " + testName + "_input " + testName + "_output"
        val result = cmd.!!
      }
    (0 until workerNum * fileNum)
      .toList.foreach { num =>
        val cmd =
          "cmd /c cd src/test/withoutNetworkTestFiles && gensort -a -b" +
            (num * fileSize).toString + " " + fileSize.toString +
            " worker" + (num/fileNum + 1).toString +
            "/" + testName + "_input/input" +
            (num + 1).toString + ".txt"
        val result = cmd.!!
      }
  }

  def checkValidity(workerNum: Int, fileNum: Int, fileSize: Int, testName: String, filePaths: List[String]): Boolean = {
    val fileNames = filePaths.map{path => path.substring(path.lastIndexOf('/') + 1)}
    (1 to workerNum)
      .toList.foreach { num =>
        val cmd =
          "cmd /c " +
            "cd src/test/withoutNetworkTestFiles && " +
            "valsort worker" + num + "/" + testName + "_output/" + fileNames(num - 1)
        val result = cmd.!!
        println(result)
      }
    val cmd1 =
      "cmd /c " +
      "type " + filePaths.mkString(" ").replaceAll("/", "\\\\") + " > src/test/withoutNetworkTestFiles/mergedFile".replaceAll("/", "\\\\")
    cmd1.!!
    val cmd2 =
      "cmd /c cd src/test/withoutNetworkTestFiles && valsort mergedFile"
    val result = cmd2.!!
    println(result)
    Files.size(Paths.get("src/test/withoutNetworkTestFiles/mergedFile")) == workerNum * fileNum * fileSize * 100
  }

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

  def automaticTest(workerNum: Int, fileNum: Int, fileSize: Int, testName: String): Unit = {
    createFiles(workerNum, fileNum, fileSize, testName)

    val startTime = System.nanoTime()

    val args = createArgs(workerNum, testName)

    val workers = args.map { arg => new WorkerLogic(new worker.Config(arg))}

    val offset = workers.map(worker => worker.getFileSize()).sum / fileSize

    val samples = workers.flatMap(worker => worker.getSampleList(offset.toInt))

    val sortedSample = samples.sorted
    val pivots =
      (1 until workerNum).toList.map(num =>sortedSample(sortedSample.length / workerNum * num))

    val fromN = workers.map(worker => worker.getToWorkerNFilePaths(new Pivots(pivots)))

    val toN = for {
      n <- (0 until workerNum).toList
      toN = fromN.map(_(n))
    } yield toN

    val resultFilePaths = workers.zipWithIndex.map{case (worker, index) => worker.mergeWrite(index, toN(index).flatten)}

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e6
    println(s"test time : $duration ms")

    checkValidity(workerNum, fileNum, fileSize, testName, resultFilePaths)
  }

  createFiles(2, 2, 1000, "4000")
  val workerArg = createArgs(2, "4000")
  val worker1Arg = workerArg(0)
  val worker2Arg = workerArg(1)

  test("sortSmallFile test : sorted correctly ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val sortedDatas = worker1.sortedSmallFilePaths.map(path => worker1.readFile(path))
    assert(sortedDatas.forall(entities => isDataSorted(entities)))
  }

  test("produceSampleFile test : subset ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val originalData = worker1.readFile(worker1.sortedSmallFilePaths.head)
    val sampleFilePath = worker1.produceSampleFile(worker1.sortedSmallFilePaths.head, 100)
    val sampleData = worker1.readFile(sampleFilePath)
    assert(sampleData.forall(entity => originalData.contains(entity)))
  }

  test("produceSampleFile test : sorted ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val sampleFilePath = worker1.produceSampleFile(worker1.sortedSmallFilePaths.head, 100)
    val sampleData = worker1.readFile(sampleFilePath)
    assert(isDataSorted(sampleData))
  }

  test("produceSampleFile test : length ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val originalData = worker1.readFile(worker1.sortedSmallFilePaths.head)
    val sampleFilePath = worker1.produceSampleFile(worker1.sortedSmallFilePaths.head, 100)
    val sampleData = worker1.readFile(sampleFilePath)
    assert(sampleData.length == (originalData.length + 100 - 1) / 100)
  }

  test("sampleFilesToSampleList test : length ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val sampleFilePaths = worker1.sortedSmallFilePaths.map(path => worker1.produceSampleFile(path, 100))
    val sampleDatas = sampleFilePaths.map(path => worker1.readFile(path))
    val sampleList = worker1.sampleFilesToSampleList(sampleFilePaths)
    assert(sampleList.length == sampleDatas.map(_.length).sum)
  }

  test("makePartitionedFiles test : N Files ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val partitionedFiles = worker1.makePartitionedFiles(worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
    assert(partitionedFiles.length == 2)
  }

  test("makePartitionedFiles test : length ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val partitionedFiles = worker1.makePartitionedFiles(worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
    assert(partitionedFiles.map(path => worker1.readFile(path).length).sum == worker1.readFile(worker1.sortedSmallFilePaths.head).length)
  }

  test("makePartitionedFiles test : sort ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val partitionedFiles = worker1.makePartitionedFiles(worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
    val partitionedDatas = partitionedFiles.map(path => worker1.readFile(path))
    assert(partitionedDatas.forall(entities => isDataSorted(entities)))
  }

  test("mergeTwoFile test: length ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val originalLength = worker1.sortedSmallFilePaths.map(path => worker1.readFile(path).length).sum
    val mergedFilePath = worker1.mergeTwoFile(0, worker1.sortedSmallFilePaths.head, worker1.sortedSmallFilePaths(1))
    val data = worker1.readFile(mergedFilePath)
    assert(data.length == originalLength)
  }

  test("mergeTwoFile test: sort ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val mergedFilePath = worker1.mergeTwoFile(0, worker1.sortedSmallFilePaths.head, worker1.sortedSmallFilePaths(1))
    val data = worker1.readFile(mergedFilePath)
    assert(isDataSorted(data))
  }

  test("mergeWrite test : length ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val originalLength = worker1.sortedSmallFilePaths.map(path => worker1.readFile(path).length).sum
    val mergedFile = worker1.mergeWrite(1, worker1.sortedSmallFilePaths)
    val mergedLength = worker1.readFile(mergedFile).length
    assert(originalLength == mergedLength)
  }

  test("mergeStreams test : sorted ") {
    val args1 = worker1Arg
    val worker1 = new WorkerLogic(new worker.Config(args1))
    val mergedFile = worker1.mergeWrite(1, worker1.sortedSmallFilePaths)
    val mergedData = worker1.readFile(mergedFile)
    assert(isDataSorted(mergedData))
  }

  test("overall correctness : total 4000 entities ") {
    // [1st] test time : 121.8784 ms
    // [2nd] test time : 125.1309 ms
    automaticTest(2, 2, 1000, "4000")
  }

  test("32MB x 2, 2 worker ") {
    // [1st] test time : 13914.6475 ms
    // [2nd] test time : 14185.2502 ms
    automaticTest(2, 2, 320000, "big2")
  }

  test("32MB x 10, 2 worker ") {
    // [1st] test time : 49565.1153 ms
    // [2nd] test time : 53844.3225 ms
    automaticTest(2, 10, 320000, "big10")
  }

  test("32MB x 10, 10 worker ") {
    // [1st] test time : 308135.1816 ms
    // [2nd] test time : 253540.936 ms
    automaticTest(10, 10, 320000, "real10")
  }
}
