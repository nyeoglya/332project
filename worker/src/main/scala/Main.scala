package worker

import org.rogach.scallop._
import zio._
import zio.stream._
import java.io.File
import scala.io.Source
import java.nio.file.Files
import common.Entity

class Config(args: Seq[String]) extends ScallopConf(args) {
  val masterAddress = trailArg[String](required = true, descr = "Master address (e.g., 192.168.0.1:8080)")
  val inputDirectories = opt[List[String]](name = "I", descr = "Input directories", default = Some(List()))
  val outputDirectory = opt[String](name = "O", descr = "Output directory", default = Some(""))
  verify()
}

object Main extends App {
  val config = new Config(args)

  println(s"Master Address: ${config.masterAddress()}")
  config.inputDirectories.toOption.foreach(dirs => println(s"Input Directories: ${dirs.mkString(", ")}"))
  config.outputDirectory.toOption.foreach(dir => println(s"Output Directory: $dir"))

  def sortSmallFile(filePath : String) : String = ???
  def produceSampleFile(filePath : String, offset : Int) : String = ???
  def sampleFilesToSampleStream(filePaths : List[String]) : Stream[Exception, Entity] = ???
  def splitFileIntoPartitionStreams(filePath : String) : List[Stream[Exception, Entity]] = ???
  def mergeBeforeShuffle(partitionStreams : List[Stream[Exception, Entity]]) : Stream[Exception, Entity] = ???
  def mergeAfterShuffle(workerStreams : List[Stream[Exception, Entity]]) : String = ???

  val machineNumber : Int = ???
  val numberOfFiles : Int = ???
  val pivotList : List[String] = ???
  val workerIpList : List[String] = ???
  val sampleOffset : Int = ???

  val originalSmallFilePaths : List[String] = ???
  val sortedSmallFilePaths : List[String] = ???
  val sampleFilePaths : List[String] = ???
  val sampleStream : Stream[Exception, Entity] = ???
  val partitionStreams : List[List[Stream[Exception, Entity]]] = ???
  val beforeShuffleStreams : List[Stream[Exception, Entity]] = ???
  val afterShuffleStreams : List[Stream[Exception, Entity]] = ???
  val mergedFilePath : String = ???

}
