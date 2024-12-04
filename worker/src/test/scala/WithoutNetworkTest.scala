package worker

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.rogach.scallop._
import Main._
import common._
import proto.common.{Entity, Pivots}
import zio.stream.{ZPipeline, ZStream}
import zio.{Ref, Runtime, Unsafe, ZIO, ZLayer}

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

  val parallelMode = false

  val os = System.getProperty("os.name").toLowerCase

  def useParallelism[A, B](target: List[A])(func: A => B): List[B] = {
    val parallel: ZIO[Any, Throwable, List[B]] =
      ZIO.foreachPar(target)(elem => ZIO.succeed(func(elem)))
    if(parallelMode) {
      Unsafe unsafe {implicit unsafe =>
        Runtime.default.unsafe.run(parallel).getOrThrow()
      }
    }
    else target.map(func)
  }

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
        val sortedSmallFolder = new File("src/test/withoutNetworkTestFiles/worker" + num.toString + "/sortedSmall")
        val partitionedFolder = new File("src/test/withoutNetworkTestFiles/worker" + num.toString + "/partitioned")
        val mergingFolder = new File("src/test/withoutNetworkTestFiles/worker" + num.toString + "/merging")
        val outputFolder = new File("src/test/withoutNetworkTestFiles/worker" + num.toString + "/" + testName + "_output")
        val inputFolderCmd =
          if(inputFolder.exists() && inputFolder.isDirectory)
            "rmdir /s /q " + testName + "_input"
          else "cd ."
        val sortedSmallFolderCmd =
          if(sortedSmallFolder.exists() && sortedSmallFolder.isDirectory)
            "rmdir /s /q sortedSmall"
          else "cd ."
        val partitionedFolderCmd =
          if(partitionedFolder.exists() && partitionedFolder.isDirectory)
            "rmdir /s /q partitioned"
          else "cd ."
        val mergingFolderCmd =
          if(mergingFolder.exists() && mergingFolder.isDirectory)
            "rmdir /s /q merging"
          else "cd ."
        val outputFolderCmd =
          if(outputFolder.exists() && outputFolder.isDirectory)
            "rmdir /s /q " + testName + "_output"
          else "cd ."
        val cmd =
          "cmd /c " +
            "cd src/test/withoutNetworkTestFiles/worker" + num.toString + " && " +
            inputFolderCmd + " && " + outputFolderCmd + " && " + sortedSmallFolderCmd + " && " + partitionedFolderCmd + " && " + mergingFolderCmd + " && " +
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
            (num + 1).toString// + ".txt"
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
    val end0 = System.nanoTime()
    val offset = workers.map(worker => worker.getFileSize).sum / (workerNum * fileNum * 1000) + 1
    val end1 = System.nanoTime()
    val samples = useParallelism(workers)(_.getSampleList(offset.toInt))
    val end2 = System.nanoTime()
    val flatSamples: List[String] = samples.flatten
    val sortedSample: List[String] = flatSamples.sorted

    val pivots = (1 until workerNum).toList.map(num =>sortedSample(num * sortedSample.length / workerNum))
    val end3 = System.nanoTime()
    val fromN = useParallelism(workers.zipWithIndex) { case (worker, index) =>
      worker.getToWorkerNFilePaths(index, new Pivots(pivots))
    }

    val end4 = System.nanoTime()
    val toN: List[List[List[String]]] =
      (for {
        n <- (0 until workerNum).toList
        toN = fromN.map(_(n))
      } yield toN)

    val resultFilePaths = useParallelism(workers.zipWithIndex){case (worker, index) =>
      toN(index).patch(index, Nil, 1).flatten[String].filter(path => path != "").map{path => worker.writeNetworkFile(worker.readFile(path))}
      worker.mergeWrite(index)
    }
     // workers.zipWithIndex.map{case (worker, index) => worker.mergeWrite(index, toN(index).flatten)}
    val end5 = System.nanoTime()
    val workerTime = (end0 - startTime) / 1e6
    val offsetTime = (end1 - end0) / 1e6
    val sampleTime = (end2 - end1) / 1e6
    val pivotTime = (end3 - end2) / 1e6
    val fromTime = (end4 - end3) / 1e6
    val mergeTime = (end5 - end4) / 1e6
    val totalTime = (end5 - startTime) / 1e6
    println(s"test time: $totalTime ms")
    println(s"worker: $workerTime ms | offset: $offsetTime ms | sample: $sampleTime ms")
    println(s"pivot: $pivotTime ms | from: $fromTime ms | merge: $mergeTime ms")

    checkValidity(workerNum, fileNum, fileSize, testName, resultFilePaths)
  }

  if(os.contains("win")) {
    val workerArg = createArgs(2, "4000")
    val worker1Arg = workerArg(0)
    val worker2Arg = workerArg(1)

    test("sortSmallFile test : sorted correctly ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      val sortedDatas = worker1.sortedSmallFilePaths.map(path => worker1.readFile(path))
      assert(sortedDatas.forall(entities => isDataSorted(entities)))
    }

    test("produceSampleFile test : subset ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      val originalData = worker1.readFile(worker1.sortedSmallFilePaths.head)
      val sampleData = worker1.produceSample(worker1.sortedSmallFilePaths.head, 100)
      assert(sampleData.forall(head => originalData.exists(_.head == head)))
    }

    test("produceSampleFile test : sorted ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      val sampleData = worker1.produceSample(worker1.sortedSmallFilePaths.head, 100)
      assert(sampleData == sampleData.sorted)
    }

    test("produceSampleFile test : length ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      val originalData = worker1.readFile(worker1.sortedSmallFilePaths.head)
      val sampleData = worker1.produceSample(worker1.sortedSmallFilePaths.head, 100)
      assert(sampleData.length == (originalData.length + 100 - 1) / 100)
    }

    test("makePartitionedFiles test : N Files ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      val partitionedFiles = worker1.makePartitionedFiles(0, worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
      assert(partitionedFiles.length == 2)
    }

    test("makePartitionedFiles test : length ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      val partitionedFiles = worker1.makePartitionedFiles(0, worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
      assert(partitionedFiles.map(paths => paths.map(worker1.readFile(_).length).sum).sum == worker1.readFile(worker1.sortedSmallFilePaths.head).length)
    }

    test("makePartitionedFiles test : sort ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      val partitionedFiles = worker1.makePartitionedFiles(0, worker1.sortedSmallFilePaths.head, List("FP]Wi|7_W9"))
      val partitionedDatas = partitionedFiles.map(_.flatMap(worker1.readFile))
      assert(partitionedDatas.forall(entities => isDataSorted(entities)))
    }

    test("mergeTwoFile test: length ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      val originalLength = worker1.sortedSmallFilePaths.map(path => worker1.readFile(path).length).sum
      val mergedFilePath = worker1.mergeTwoFile(worker1.sortedSmallFilePaths.head, worker1.sortedSmallFilePaths(1), worker1.PathMaker.resultFile(0))
      val data = worker1.readFile(mergedFilePath)
      assert(data.length == originalLength)
    }

    test("mergeTwoFile test: sort ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      val mergedFilePath = worker1.mergeTwoFile(worker1.sortedSmallFilePaths.head, worker1.sortedSmallFilePaths(1), worker1.PathMaker.resultFile(0))
      val data = worker1.readFile(mergedFilePath)
      assert(isDataSorted(data))
    }

    test("mergeWrite test : length ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      val originalLength = worker1.sortedSmallFilePaths.map(path => worker1.readFile(path).length).sum
      worker1.sortedSmallFilePaths.foreach(path => worker1.writeNetworkFile(worker1.readFile(path)))
      val mergedFile = worker1.mergeWrite(1)
      val mergedLength = worker1.readFile(mergedFile).length
      assert(originalLength == mergedLength)
    }

    test("mergeStreams test : sorted ") {
      createFiles(2, 2, 1000, "4000")
      val worker1 = new WorkerLogic(new worker.Config(worker1Arg))
      worker1.sortedSmallFilePaths.foreach(path => worker1.writeNetworkFile(worker1.readFile(path)))
      val mergedFile = worker1.mergeWrite(1)
      val mergedData = worker1.readFile(mergedFile)
      assert(isDataSorted(mergedData))
    }

    test("overall correctness : total 4000 entities ") {
      // before parallelization
      // test time: 159.1675 ms
      // worker: 8.8808 ms | offset: 1.3975 ms | sample: 107.8371 ms
      // pivot: 0.2801 ms | from: 5.8402 ms | merge: 34.9318 ms

      // test time: 50.4675 ms
      // worker: 7.0514 ms | offset: 1.3292 ms | sample: 12.685 ms
      // pivot: 0.2675 ms | from: 4.0958 ms | merge: 25.0386 ms
      automaticTest(2, 2, 1000, "4000")
    }

    test("10 entities x 5, 10 worker ") {
      automaticTest(10, 5, 10, "small10")
    }

    test("2 entities x 1, 2 worker ") {
      automaticTest(2, 1, 2, "verySmall")
    }

    test("32MB x 2, 2 worker ") {
      // before parallelization
      // test time: 2672.6841 ms
      // worker: 1342.2287 ms | offset: 0.2474 ms | sample: 337.8965 ms
      // pivot: 0.1082 ms | from: 262.5807 ms | merge: 729.6226 ms

      // test time: 1767.579 ms
      // worker: 899.224 ms | offset: 0.2581 ms | sample: 222.2038 ms
      // pivot: 0.1433 ms | from: 181.1034 ms | merge: 464.6464 ms
      automaticTest(2, 2, 320000, "small2")
    }

    test("32MB x 10, 2 worker ") {
      // before parallelization
      // test time: 10500.2962 ms
      // worker: 4484.7662 ms | offset: 0.4556 ms | sample: 679.959 ms
      // pivot: 0.2872 ms | from: 818.1955 ms | merge: 4516.6327 ms

      // test time: 4819.1306 ms
      // worker: 1237.4412 ms | offset: 0.6212 ms | sample: 462.0491 ms
      // pivot: 0.2984 ms | from: 389.9135 ms | merge: 2728.8072 ms
      automaticTest(2, 10, 320000, "big10")
    }

    test("32MB x 2, 10 worker ") {
      automaticTest(10, 2, 320000, "small")
    }

    test("32MB x 10, 10 worker ") {
      // before parallelization
      // test time: 40812.3332 ms
      // worker: 22491.0053 ms | offset: 1.8241 ms | sample: 1508.7812 ms
      // pivot: 0.3917 ms | from: 1697.7231 ms | merge: 15112.6078 ms

      // test time: 22083.3851 ms
      // worker: 4792.2191 ms | offset: 2.1347 ms | sample: 1496.6731 ms
      // pivot: 0.4116 ms | from: 1732.62 ms | merge: 14059.3266 ms
      automaticTest(10, 10, 320000, "big")
    }

    test("32MB x 100, 10 worker ") {
      automaticTest(10, 100, 320000, "large")
    }

  }
  test("dummy") {
    assert(true)
  }
}
