package worker

import org.rogach.scallop._
import zio._
import zio.stream._
import zio.Task

import java.io.{File, PrintWriter}
import scala.io.Source
import java.nio.file.Files
import common.Entity

import scala.collection.mutable.PriorityQueue

//import scala.concurrent.Promise
//import scala.concurrent.Future
//import scala.concurrent.ExecutionContext.Implicits.global

class Config(args: Seq[String]) extends ScallopConf(args) {
  val masterAddress = trailArg[String](required = true, descr = "Master address (e.g., 192.168.0.1:8080)")
  val inputDirectories = opt[List[String]](name = "I", descr = "Input directories", default = Some(List()))
  val outputDirectory = opt[String](name = "O", descr = "Output directory", default = Some(""))
  verify()
}

object Mode {
  val testMode = true
  def setMode() = ()
}

/** Worker's Main function
 *
 *  모드에 따라 파일의 양식, path 위치가 다르기에, 편의를 위해 [[readFile]], [[writeFile]], [[PathMaker]] 를 정의해 사용하였다.
 *
 *  주요 함수들은 아래와 같다.
 *
 *  [[sortSmallFile]]
 *  [[produceSampleFile]]
 *  [[sampleFilesToSampleStream]]
 *  [[splitFileIntoPartitionStreams]]
 *  [[mergeBeforeShuffle]]
 *  [[mergeAfterShuffle]]
 *
 *  각각은 그저 일반 함수처럼 동작한다.
 *  위의 함수들을 병렬적으로 돌리고, 기다리는 작업은 밑의 "real workflow begin from here" 부분에서 시행한다.
 *
 *  TODO #1 : 병렬화
 *
 *  TODO #2 : 네트워크 서비스와의 연결
 *
 *  TODO #3 : 중간 파일 지우기
 *
 */
object Main extends App {
  val config = new Config(args)

  println(s"Master Address: ${config.masterAddress()}")
  config.inputDirectories.toOption.foreach(dirs => println(s"Input Directories: ${dirs.mkString(", ")}"))
  config.outputDirectory.toOption.foreach(dir => println(s"Output Directory: $dir"))

  Mode.setMode()


  object PathMaker {
    def sortedSmallFile(filePath : String) : String = {
      val index = filePath.lastIndexOf('/')
      if(Mode.testMode)
        filePath.substring(0, index + 1) + "sorted_" + filePath.substring(index + 1)
      else
        config.outputDirectory + "/" + "sorted_" + filePath.substring(index + 1)
    }
    def sampleFile(filePath : String) : String = {
      val index = filePath.lastIndexOf('/')
      if(Mode.testMode)
        filePath.substring(0, index + 1) + "sampled_" + filePath.substring(index + 1)
      else
        config.outputDirectory + "/" + "sampled_" + filePath.substring(index + 1)
    }
    def mergedFile() : String = {
      if(Mode.testMode) "src/test/funcTestFiles/output/mergedFile.txt"
      else config.outputDirectory + "/mergedFile" + machineNumber.toString
    }
  }

  /** read file from filePath and return its data in a list of Entity
   *  depending on mode, it works differently
   *
   * @param filePath path of the file to read
   * @return data of the file read
   */
  def readFile(filePath : String) : List[Entity] = {
    def readTestFile(filePath : String) : List[Entity] = {
      val source = Source.fromFile(filePath)
      val lines = source.getLines().toList
      source.close()
      for {
        line <- lines
        words = line.split("\\|")
      } yield Entity(words(0), words(1))
    }
    def readRealFile(filePath : String) : List[Entity] = {
      val source = Source.fromFile(filePath)
      val lines = source.grouped(100).toList
      source.close()
      for {
        line <- lines
        (key, value) = line.splitAt(10)
      } yield Entity(key.toString(), value.toString())
    }
    if(Mode.testMode) readTestFile(filePath)
    else readRealFile(filePath)
  }

  /** write data to new file
   *  depending on mode, it works differently
   *
   * @param filePath path of the file to write
   * @param data a list of entity to write
   */
  def writeFile(filePath : String, data : List[Entity]) : Unit = {
    def writeTestFile(filePath : String, data : List[Entity]) : Unit = {
      val file = new File(filePath)
      val writer = new PrintWriter(file)
      try {
        data.foreach(entity => {
          writer.write(entity.head)
          writer.write("|")
          writer.write(entity.body)
          writer.write("\n")
        })
      } finally {
        writer.close()
      }
    }
    def writeRealFile(filePath : String, data : List[Entity]) : Unit = {
      val file = new File(filePath)
      val writer = new PrintWriter(file)
      try {
        data.foreach(entity => {
          writer.write(entity.head)
          writer.write(entity.body)
        })
      } finally {
        writer.close()
      }
    }
    if(Mode.testMode) writeTestFile(filePath, data)
    else writeRealFile(filePath, data)
  }

  /** write data to new file through stream
   *  depending on mode, it works differently
   *
   *  because size of file is limited when we write file in a normal way,
   *  we introduce stream to write huge entities on one single file
   *
   * @param filePath path of the file to write
   * @param data a list of entity to write(huge)
   */
  def writeFileViaStream(filePath : String, data : Stream[Exception, Entity]) : Unit = {
    def writeTestFileViaStream(filePath : String, data : Stream[Exception, Entity]) : Unit = {
      val file = new File(filePath)
      val writer = new PrintWriter(file)
      try {
        Unsafe unsafe {
          implicit unsafe =>
            Runtime.default.unsafe.run{
              data.runForeach(entity => {
                writer.write(entity.head)
                writer.write("|")
                writer.write(entity.body)
                writer.write("\n")
                ZIO.succeed(())
              })
            }
        }
      } finally {
        writer.close()
      }
    }
    def writeRealFileViaStream(filePath : String, data : Stream[Exception, Entity]) : Unit = {
      val file = new File(filePath)
      val writer = new PrintWriter(file)
      try {
        Unsafe unsafe {
          implicit unsafe =>
            Runtime.default.unsafe.run{
              data.runForeach(entity => {
                writer.write(entity.head)
                writer.write(entity.body)
                ZIO.succeed(())
              })
            }
        }
      } finally {
        writer.close()
      }
    }
    if(Mode.testMode) writeTestFileViaStream(filePath, data)
    else writeRealFileViaStream(filePath, data)
  }

  /** this function extract entity from ZIO value
   *
   * @param zio original ZIO value
   * @return entity that had been in ZIO
   */
  def zio2entity(zio : ZIO[Any, Exception, Entity]) : Entity =
    Unsafe unsafe {
      implicit unsafe =>
        Runtime.default.unsafe.run(zio).getOrThrow()
    }

  /** this function literally sort small File
   *  and save sorted data in somewhere and return that path
   *
   * @param filePath original file's path
   * @return sorted file's path
   */
  def sortSmallFile(filePath : String) : String = {
    val data = readFile(filePath)
    val sortedData = data.sortBy(entity => entity.head)
    val sortedFilePath = PathMaker.sortedSmallFile(filePath)
    writeFile(sortedFilePath, sortedData)
    sortedFilePath
  }

  /** this function sample data from given file
   *  by striding @stride entities
   *
   * @param filePath given file's path(sorted)
   * @param stride
   * @return new sample file's path
   */
  def produceSampleFile(filePath : String, stride : Int) : String = {
    val data = readFile(filePath)
    val sampledData = data.zipWithIndex.collect{ case (entity, index) if index % stride == 0 => entity}
    val sampledFilePath = PathMaker.sampleFile(filePath)
    writeFile(sampledFilePath, sampledData)
    sampledFilePath
  }

  /** this function merge given sample files
   *  and return a single merged sample data(in list)
   *  here, 'merge' does not consider ordering. it just collects them
   *
   * @param filePaths list of given sample file
   * @return merged sample data
   */
  def sampleFilesToSampleStream(filePaths : List[String]) : List[Entity] = {
    filePaths.flatMap(path => readFile(path))
  }

  /** this function split given file into several mini streams using pivots
   *
   * @param filePath target file's path
   * @param pivots list of pivot
   * @return list of mini streams
   */
  def splitFileIntoPartitionStreams(filePath : String, pivots : List[String]) : List[Stream[Exception, Entity]] = {
    val data = readFile(filePath)
    def splitUsingPivots(data : List[Entity], pivots: List[String]) : List[List[Entity]] = pivots match {
      case Nil => List(data)
      case pivot::pivots =>
        data.takeWhile(entity => entity.head < pivot) :: splitUsingPivots(data.dropWhile(entity => entity.head < pivot), pivots)
    }
    splitUsingPivots(data, pivots).map(entityList => ZStream.fromIterable(entityList))
  }

  /** this function merge list of streams into a single sorted stream
   *  all streams in input want to go same worker so we'll make them one
   *  using priorityQueue, we can efficiently merge streams.
   *  to access entity in ZStream, we use zio2entity function
   *
   * @param partitionStreams list of partitioned streams
   * @return a merged stream
   */
  def mergeBeforeShuffle(partitionStreams : List[Stream[Exception, Entity]]) : Stream[Exception, Entity] = {
    implicit val entityOrdering : Ordering[(Entity, Int)] = Ordering.by(_._1.head)
    val minHeap : PriorityQueue[(Entity, Int)] = PriorityQueue.empty(entityOrdering.reverse)
    partitionStreams.zipWithIndex foreach {
      case (stream, index) =>
        val head = zio2entity(stream.runHead.map(_.getOrElse(Entity("", ""))))
        if(head != Entity("", ""))
          minHeap.enqueue((head, index))
    }
    def makeMergeStream(streams : List[Stream[Exception, Entity]], accStream : Stream[Exception, Entity]) : Stream[Exception, Entity] = {
      if(minHeap.isEmpty) accStream
      else {
        val (minEntity, minIndex) = minHeap.dequeue()
        val head = zio2entity(streams(minIndex).runHead.map(_.getOrElse(Entity("", ""))))
        val tailStream = streams(minIndex).drop(1)
        if (head != Entity("", "")) minHeap.enqueue((head, minIndex))
        makeMergeStream(streams.updated(minIndex, tailStream), accStream ++ ZStream.succeed(minEntity))
      }
    }
    makeMergeStream(partitionStreams.map(_.drop(1)), ZStream.empty)
  }

  /** this function merge list of streams into a single sorted stream(same with mergeBeforeShuffle)
   *  and write this huge stream into single file and return its path
   *
   * @param workerStreams list of streams that sent by other workers
   * @return merged file's path
   */
  def mergeAfterShuffle(workerStreams : List[Stream[Exception, Entity]]) : String = {
    val mergedStream = mergeBeforeShuffle(workerStreams)
    val filePath = PathMaker.mergedFile()
    writeFileViaStream(filePath, mergedStream)
    filePath
  }

  val machineNumber : Int = 1
  val numberOfFiles : Int = ???
  val pivotList : List[String] = ???
  val workerIpList : List[String] = ???
  val sampleOffset : Int = ???

  /// real workflow begin from here //////////////////////////////////////////////////////////////////////////////////

  val originalSmallFilePaths : List[String] =
    config.inputDirectories.toOption.getOrElse(List(""))
      .flatMap{ directoryPath =>
        val directory = new File(directoryPath)
        directory.listFiles.map(_.getPath).toList
      }

  // send data to master and wait 'getSample' command from master

  /**
   * TODO : make it parallel
   */
  val sortedSmallFilePaths : List[String] = originalSmallFilePaths.map(path => sortSmallFile(path))

  /**
   * TODO : make it parallel
   */
  val sampleFilePaths : List[String] = sortedSmallFilePaths.map(path => produceSampleFile(path, sampleOffset))

  val sampleStream : List[Entity] = sampleFilesToSampleStream(sampleFilePaths)

  // send sampleStream and wait pivotList
  // after receive pivotList, send stream request to other workers

  /**
   * TODO : make it parallel
   */
  val partitionStreams : List[List[Stream[Exception, Entity]]] =
    sortedSmallFilePaths.map(path => splitFileIntoPartitionStreams(path, pivotList))

  val toWorkerStreams = workerIpList.indices.map(n => partitionStreams.collect(_(n))).toList

  /**
   * TODO : make it parallel
   */
  val beforeShuffleStreams : List[Stream[Exception, Entity]] =
    toWorkerStreams.map(toWorkerN => mergeBeforeShuffle(toWorkerN))

  // wait until receive all stream from other workers

  val mergedFilePath : String = mergeAfterShuffle(???)
}
