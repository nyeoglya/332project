package master

import zio._
import zio.stream._
import java.time.Duration

import org.rogach.scallop._

case class WorkerData(workerIP: String, storageSize: BigInt)

class Config(args: Seq[String]) extends ScallopConf(args) {
  val workerNum = trailArg[Int](required = true, descr = "Number of workers")
  verify()
}

object Main extends App {
  val config = new Config(args)
  println(s"Number of Workers: ${config.workerNum()}")

  val workerIPList: List[String] = Nil
  val workerDataList: List[WorkerData] = Nil
  val sampleStreamList: List[ZStream[Any, Throwable, String]] = ???
  val selectedPivots: List[String] = ???

  def getSamplesFromWorker(workerIP: String): ZStream[Any, Throwable, String] = ???
  def getWorkerData(workerIP: String): WorkerData = ???
  def selectPivots(pivotStreamList: List[ZStream[Any, Throwable, String]]): List[String] = ???
  def sendPivots(pivotList: List[String]): ZIO[Any, Throwable, Unit] = ???
}
