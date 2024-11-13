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

object Main extends App {
  val config = new Config(args)

  println(s"Master Address: ${config.masterAddress()}")
  config.inputDirectories.toOption.foreach(dirs => println(s"Input Directories: ${dirs.mkString(", ")}"))
  config.outputDirectory.toOption.foreach(dir => println(s"Output Directory: $dir"))

  Mode.setMode()

  def readFile(filePath : String) : List[Entity] = {
    if(Mode.testMode) readTestFile(filePath)
    else readRealFile(filePath)
  }
  def writeFile(filePath : String, data : List[Entity]) : Unit = {
    if(Mode.testMode) writeTestFile(filePath, data)
    else writeRealFile(filePath, data)
  }
  def writeFileViaStream(filePath : String, data : Stream[Exception, Entity]) : Unit = {
    if(Mode.testMode) writeTestFileViaStream(filePath, data)
    else writeRealFileViaStream(filePath, data)
  }

  def readRealFile(filePath : String) : List[Entity] = Nil
  def writeRealFile(filePath : String, data : List[Entity]) : Unit = ()
  def writeRealFileViaStream(filePath : String, data : Stream[Exception, Entity]) : Unit = ()

  /** read file for function test
   *
   * @param filePath test file's name
   * @return list of entity
   */
  def readTestFile(filePath : String) : List[Entity] = {
    val source = Source.fromFile("src/test/scala/" + filePath)
    val lines = source.getLines().toList
    source.close()
    for {
      line <- lines
      words = line.split("\\|")
    } yield Entity(words(0), words(1))
  }

  /** write result for function test
   *
   * @param filePath new file's name
   * @param data new file's contents
   */
  def writeTestFile(filePath : String, data : List[Entity]) : Unit = {
    val file = new File("src/test/scala/" + filePath)
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

  def writeTestFileViaStream(filePath : String, data : Stream[Exception, Entity]) : Unit = {
    val file = new File("src/test/scala/" + filePath)
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
    val sortedFileName = "sorted_" + filePath
    writeFile(sortedFileName, sortedData)
    sortedFileName
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
    val sampledFileName = "sampled_" + filePath
    writeFile(sampledFileName, sampledData)
    sampledFileName
  }

  /** this function merge given sample files
   *  and return a single merged sample data(in list)
   *  here, 'merge' does not consider ordering. it just collects them
   *
   * @param filePaths list of given sample file
   * @return merged sample data
   */
  def sampleFilesToSampleStream(filePaths : List[String]) : List[Entity] = {
    filePaths.flatMap(name => readFile(name))
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
    val fileName = "mergedFile.txt"
    writeTestFileViaStream(fileName, mergedStream)
    fileName
  }

  val machineNumber : Int = 1
  val numberOfFiles : Int = ???
  val pivotList : List[String] = ???
  val workerIpList : List[String] = ???
  val sampleOffset : Int = ???

  val originalSmallFilePaths : List[String] = ???
  val sortedSmallFilePaths : List[String] = ???
  val sampleFilePaths : List[String] = ???
  val sampleStream : List[String] = ???
  val partitionStreams : List[List[Stream[Exception, Entity]]] = ???
  val beforeShuffleStreams : List[Stream[Exception, Entity]] = ???
  val afterShuffleStreams : List[Stream[Exception, Entity]] = ???
  val mergedFilePath : String = ???
}
