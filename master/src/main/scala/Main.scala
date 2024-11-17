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

class Config(args: Seq[String]) extends ScallopConf(args) {
  val workerNum = trailArg[Int](required = true, descr = "Number of workers", default = Some(1))
  verify()
}

object Main extends ZIOAppDefault {
  def port: Int = 7080

  override def run: ZIO[Environment with ZIOAppArgs with Scope,Any,Any] = (for {
    _ <- zio.Console.printLine(s"Master is running on port ${port}")
    result <- serverLive.launch.exitCode
  } yield result).provideSomeLayer[ZIOAppArgs](
    ZLayer.fromZIO( for {
        args <- getArgs
        config = new Config(args)
      } yield config
    ) >>> ZLayer.fromFunction {config: Config => new MasterLogic(config)}
  )

  def builder = ServerBuilder
    .forPort(port)
    .addService(ProtoReflectionService.newInstance())

  def serverLive: ZLayer[MasterLogic, Throwable, zio_grpc.Server] = for {
    service <- ZLayer.service[MasterLogic]
    result <- zio_grpc.ServerLayer.fromServiceList(builder, zio_grpc.ServiceList.add(new ServiceImpl(service.get)))
  } yield result

  class ServiceImpl(service: MasterLogic) extends MasterService {
    def sendWorkerData(request: WorkerData): IO[StatusException,WorkerDataResponse] = {
      service.addClient(request.workerAddress, request.fileSize)
      ZIO.succeed(WorkerDataResponse())
    }
  }
}

////////////////////////////////////////////////////////////////

class MasterLogic(config: Config) {
  case class Client(
    val client: Layer[Throwable, WorkerServiceClient],
    val size: BigInt
  )
  var clients: List[Client] = List()

  lazy val offset = ???
  lazy val workerDataList: List[WorkerData] = ???

  /** Add new client connection to MasterLogic
    *
    * @param clientAddress address of client
    */
  def addClient(clientAddress: String, clientSize: BigInt) {
    println(s"New client[${clients.size}] attached: ${clientAddress}, Size: ${clientSize} Bytes")
    val address = AddressParser.parse(clientAddress).get
    clients = clients :+ Client(WorkerServiceClient.live(
      ZManagedChannel(
        ManagedChannelBuilder.forAddress(address._1, address._2).usePlaintext()
      )
    ), clientSize)
    if (clients.size == config.workerNum.toOption.get) this.run()
  }

  def collectSamples(): List[Stream[Throwable,Entity]] = ???
  def selectPivots(samples: List[Stream[Throwable,Entity]]): Pivots = ???

  def run() = {
    println("Started!")
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
  
  /*def collectSample(offset: BigInt, client: Layer[Throwable, WorkerServiceClient]): IO[Throwable, List[String]] =
    ZIO.serviceWithZIO[WorkerServiceClient] { workerServiceClient =>
      workerServiceClient.getSamples(SampleRequest(offset))
  }*/

  /*def selectPivots(pivotCandicateList: List[String], workerDataList: List[WorkerData]): Pivots = {
    val totalStorageSize = workerDataList.map(_.storageSize).sum

    val pivotIndices = workerDataList.scanLeft(BigInt(0)) { (acc, worker) =>
      acc + (worker.storageSize * pivotCandicateList.size) / totalStorageSize
    }.tail

    val distinctPivots = pivotIndices.map(idx => pivotCandicateList(idx.toInt)).distinct
    
    Pivots(pivots = distinctPivots)
  }*/

  def sendPartitionToWorker(client: Layer[Throwable, WorkerServiceClient], pivots: Pivots): Task[Unit] = ???
}
