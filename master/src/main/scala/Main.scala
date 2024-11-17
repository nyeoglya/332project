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
import proto.common.{WorkerDataRequest, WorkerDataResponse}

case class WorkerData(workerIP: String, storageSize: BigInt)

class Config(args: Seq[String]) extends ScallopConf(args) {
  val workerNum = trailArg[Int](required = true, descr = "Number of workers")
  verify()
}

object Main extends ZIOAppDefault {
  def port: Int = 7080

  override def run: ZIO[Environment with ZIOAppArgs with Scope,Any,Any] = for {
    args <- getArgs
    config = new Config(args)
    result = new MasterLogic(config).run()
  } yield result

  def builder = ServerBuilder
    .forPort(port)
    .addService(ProtoReflectionService.newInstance())

  def serverLive: ZLayer[MasterLogic, Throwable, zio_grpc.Server] = for {
    service <- ZLayer.service[MasterLogic]
    result <- zio_grpc.ServerLayer.fromServiceList(builder, zio_grpc.ServiceList.add(new ServiceImpl(service.get)))
  } yield result

  class ServiceImpl(service: MasterLogic) extends MasterService {
    def sendWorkerData(request: WorkerDataRequest): IO[StatusException,WorkerDataResponse] = ???
  }
}

////////////////////////////////////////////////////////////////

class MasterLogic(config: Config) {
  var clients: List[Layer[Throwable, WorkerServiceClient]] = List()
  lazy val offset = ???
  lazy val workerDataList: List[WorkerData] = ???

  def addClient(clientAddress: String): Boolean = {
    clients = clients :+ WorkerServiceClient.live(
      ZManagedChannel(
        ManagedChannelBuilder.forAddress(clientAddress, 8080).usePlaintext()
      )
    )
    
    return (clients.size == config.workerNum.toOption.get)
  }

  def collectSamples(): List[Stream[Throwable,Entity]] = ???
  def selectPivots(samples: List[Stream[Throwable,Entity]]): Pivots = ???

  def run() = {
    // TODO: Collect samples from workers and select pivot
    // val partition = selectPivots(collectSamples())
    // TODO: Iterate clientLayers and send partition datas
  } 

  /// Below is just a example. please make List of clientLayers from Config
  def clientLayers: List[Layer[Throwable,WorkerServiceClient]] = List(
    WorkerServiceClient.live(
      ZManagedChannel(
        ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext()
      )
    )
  )
  
  def collectSample(offset: BigInt, client: Layer[Throwable, WorkerServiceClient]): List[String] =
    ZIO.serviceWithZIO[WorkerServiceClient] { workerServiceClient =>
      workerServiceClient.getSamples(SampleRequest(offset))
  }

  def selectPivots(pivotCandicateList: List[String], workerDataList: List[WorkerData]): Pivots = {
    val totalStorageSize = workerDataList.map(_.storageSize).sum

    val pivotIndices = workerDataList.scanLeft(BigInt(0)) { (acc, worker) =>
      acc + (worker.storageSize * pivotCandicateList.size) / totalStorageSize
    }.tail

    val distinctPivots = pivotIndices.map(idx => pivotCandicateList(idx.toInt)).distinct
    
    Pivots(pivots = distinctPivots)
  }

  def sendPartitionToWorker(client: Layer[Throwable, WorkerServiceClient], pivots: Pivots): Task[Unit] = ???
}
