package master

import zio._
import zio.stream._
import java.time.Duration

import org.rogach.scallop._
import scalapb.zio_grpc
import scalapb.zio_grpc.ZManagedChannel
import io.grpc.ManagedChannelBuilder
import proto.common.ZioCommon.WorkerServiceClient
import proto.common.Entity
import proto.common.Pivots
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import proto.common.ZioCommon.MasterService
import io.grpc.StatusException
import proto.common.{WorkerData, WorkerDataResponse}
import common.AddressParser
import proto.common.SampleRequest

class MasterLogic(config: Config) {
  case class WorkerClient(val client: Layer[Throwable, WorkerServiceClient], val size: BigInt)

  var clients: List[WorkerClient] = List()

  lazy val offset = ???
  lazy val workerDataList: List[WorkerData] = ???

  /** Add new client connection to MasterLogic
    *
    * @param clientAddress address of client
    */
  def addClient(clientAddress: String, clientSize: BigInt) {
    println(s"New client[${clients.size}] attached: ${clientAddress}, Size: ${clientSize} Bytes")
    val address = AddressParser.parse(clientAddress).get
    clients = clients :+ WorkerClient(WorkerServiceClient.live(
      ZManagedChannel(
        ManagedChannelBuilder.forAddress(address._1, address._2).usePlaintext()
      )
    ), clientSize)
    if (clients.size == config.workerNum.toOption.get) this.run()
  }

  def run() = {
    // TODO: Collect samples from workers and select pivot
    // val partition = selectPivots(collectSamples())
    // TODO: Iterate clientLayers and send partition datas
    val pivotCandicateList: ZIO[Any, Throwable, List[Pivots]] = ZIO.foreachPar(clientLayers) { layer =>
      collectSample(layer).provideLayer(layer)
    }

    val selectedPivots = selectPivots(pivotCandicateList)

    // TODO: selectedPivots를 모든 Worker에 전송한다.
  }
  
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

    val pivotIndices = workerDataList.scanLeft(0) { (acc, worker) =>
      acc + pivotCandicateListSize * (worker.dataCount / totalDataCount)
    }.tail

    for {
      pivotList <- pivotCandicateList
      distinctList = pivotIndices.map(idx => pivotList(idx)).distinct
    } yield Pivots(pivots = distinctList)
  }

  def sendPartitionToWorker(client: Layer[Throwable, WorkerServiceClient],
    pivots: ZIO[Any, Throwable, Pivots]): ZIO[Any, Throwable, Unit] = for {
    pivotsData <- pivots
    workerServiceClient <- client.build
    sortRequest = SortRequest(pivots = Some(pivotsData), workerAddresses = workerDataList.flatMap(_.workerIP))
      _ <- ZIO.fromFuture { implicit ec =>
      workerServiceClient.startSort(sortRequest).map(_ => ())
    }.mapError(e => new RuntimeException(s"Failed to send SortRequest: ${e.getMessage}"))
  } yield ()
}
