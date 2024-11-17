package master

import zio._
import zio.stream._
import java.time.Duration

import org.rogach.scallop._
import scalapb.zio_grpc.ZManagedChannel
import io.grpc.ManagedChannelBuilder
import proto.common.ZioCommon.WorkerServiceClient
import proto.common.Entity
import proto.common.Pivots

case class WorkerData(workerIP: String, storageSize: BigInt)

class Config(args: Seq[String]) extends ScallopConf(args) {
  val workerNum = trailArg[Int](required = true, descr = "Number of workers")
  verify()
}

trait MasterServiceLogic {
  def clientLayers: List[Layer[Throwable, WorkerServiceClient]]
  def collectSamples(): List[Stream[Throwable, Entity]]
  def selectPivots(samples: List[Stream[Throwable, Entity]]): Pivots

  def run() = {
    // TODO: Collect samples from workers and select pivot
    // val partition = selectPivots(collectSamples())
    // TODO: Iterate clientLayers and send partition datas
  }
}

object Main extends ZIOAppDefault {
  override def run: ZIO[Environment with ZIOAppArgs with Scope,Any,Any] = for {
    args <- getArgs
    config = new Config(args)
    result = new MasterLogic(config).run()
  } yield result

  val workerIPList: List[String] = Nil
  val workerDataList: List[WorkerData] = Nil
  val sampleStreamList: List[ZStream[Any, Throwable, String]] = ???
  val selectedPivots: List[String] = ???

  def getSamplesFromWorker(workerIP: String): ZStream[Any, Throwable, String] = ???
  def getWorkerData(workerIP: String): WorkerData = ???
  def selectPivots(pivotStreamList: List[ZStream[Any, Throwable, String]]): List[String] = ???
  def sendPivots(pivotList: List[String]): ZIO[Any, Throwable, Unit] = ???
}

////////////////////////////////////////////////////////////////

class MasterLogic(config: Config) extends MasterServiceLogic {
  /// Below is just a example. please make List of clientLayers from Config
  def clientLayers: List[Layer[Throwable,WorkerServiceClient]] = List(
    WorkerServiceClient.live(
      ZManagedChannel(
        ManagedChannelBuilder.forAddress("lcoalhost", 8080).usePlaintext()
      )
    )
  )
  def collectSamples(): List[Stream[Throwable,Entity]] = ???
  def selectPivots(samples: List[Stream[Throwable,Entity]]): Pivots = ???
}

