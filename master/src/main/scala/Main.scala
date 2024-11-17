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

case class WorkerData(workerIP: String, dataCount: BigInt)

class Config(args: Seq[String]) extends ScallopConf(args) {
  val workerNum = trailArg[Int](required = true, descr = "Number of workers")
  verify()
}

object Main extends ZIOAppDefault {
  override def run: ZIO[Environment with ZIOAppArgs with Scope,Any,Any] = for {
    args <- getArgs
    config = new Config(args)
    result = new MasterLogic(config).run()
  } yield result
}

////////////////////////////////////////////////////////////////

class MasterLogic(config: Config) extends MasterServiceLogic {
  lazy val offset = ???
  lazy val workerDataList: List[WorkerData] = ???

  def run() = {
    // TODO: Collect samples from workers and select pivot
    // val partition = selectPivots(collectSamples())
    // TODO: Iterate clientLayers and send partition datas
    val pivotCandicateList: ZIO[Any, Throwable, List[Pivots]] = ZIO.foreach(clientLayers) { layer =>
      collectSample(offset, layer).provideLayer(layer).map(pivots => pivots)
    }

    val selectedPivots = selectPivots(pivotCandicateList)

    // TODO: selectedPivots를 모든 Worker에 전송한다.
  }

  /// Below is just a example. please make List of clientLayers from Config
  def clientLayers: List[Layer[Throwable, WorkerServiceClient]] = List(
    WorkerServiceClient.live(
      ZManagedChannel(
        ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext()
      )
    )
  )
  
  def collectSample(client: Layer[Throwable, WorkerServiceClient]): ZIO[WorkerServiceClient, Throwable, Pivots] =
    ZIO.serviceWithZIO[WorkerServiceClient] { workerServiceClient =>
      workerServiceClient.getSamples(SampleRequest(offset))
  }

  def selectPivots(pivotCandicateZIOList: ZIO[Any, Throwable, List[Pivots]]): ZIO[Any, Throwable, Pivots] = {
    val pivotCandicateList: ZIO[Any, Throwable, List[String]] = pivotsList.map { pivots =>
      pivots.flatMap(_.pivots)
    }

    val pivotCandicateListSize: ZIO[Any, Throwable, Int] = pivotCandicateList.map(_.size)
    val totalDataCount = workerDataList.map(_.dataCount).sum

    val pivotIndices = workerDataList.scanLeft(BigInt(0)) { (acc, worker) =>
      acc + pivotCandicateListSize * (worker.dataCount / totalDataCount)
    }.tail

    for {
      pivotList <- pivotCandicateList
      distinctList = pivotIndices.map(idx => pivotList(idx)).distinct
    } yield Pivots(pivots = distinctList)
  }

  def sendPartitionToWorker(client: Layer[Throwable, WorkerServiceClient], pivots: Pivots): Task[Unit] = ???
}
