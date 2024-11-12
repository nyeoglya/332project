package worker

import org.rogach.scallop._
import zio._
import zio.stream._

import java.io.{File, PrintWriter}
import scala.io.Source
import java.nio.file.Files
import common.Entity

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

  def sortSmallFile(filePath : String) : String = {
    val data =
      if(Mode.testMode) readTestFile(filePath)
      else List(Entity(" ", " "))
    val sortedData = data.sortBy(entity => entity.head)
    val sortedFileName = "sorted_" + filePath
    if(Mode.testMode) writeTestFile(sortedFileName, sortedData) else ()
    sortedFileName
  }

  def produceSampleFile(filePath : String, offset : Int) : String = {
    val data =
      if(Mode.testMode) readTestFile(filePath)
      else List(Entity(" ", " "))
    val sampledData = data.zipWithIndex.collect{ case (entity, index) if index % offset == 0 => entity}
    val sampledFileName = "sampled_" + filePath
    if(Mode.testMode) writeTestFile(sampledFileName, sampledData) else ()
    sampledFileName
  }

  def sampleFilesToSampleStream(filePaths : List[String]) : List[Entity] = {
    if(Mode.testMode) filePaths.flatMap(name => readTestFile(name))
    else List(Entity(" ", " "))
  }

  def splitFileIntoPartitionStreams(filePath : String, pivots : List[String]) : List[Stream[Exception, Entity]] = {
    val data =
      if(Mode.testMode) readTestFile(filePath)
      else List(Entity(" ", " "))
    def splitUsingPivots(data : List[Entity], pivots: List[String]) : List[List[Entity]] = pivots match {
      case Nil => List(data)
      case pivot::pivots =>
        data.takeWhile(entity => entity.head < pivot) :: splitUsingPivots(data.dropWhile(entity => entity.head < pivot), pivots)
    }
    splitUsingPivots(data, pivots).map(entityList => ZStream.fromIterable(entityList))
  }

  def mergeBeforeShuffle(partitionStreams : List[Stream[Exception, Entity]]) : Stream[Exception, Entity] = ???
  def mergeAfterShuffle(workerStreams : List[Stream[Exception, Entity]]) : String = ???

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
