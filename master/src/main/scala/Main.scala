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
import proto.common.ShuffleRequest
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import proto.common.ZioCommon.MasterService
import io.grpc.StatusException
import proto.common.{WorkerData, WorkerDataResponse}
import common.AddressParser
import proto.common.SampleRequest
import io.grpc.Status

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
      val result = for {
        addClient <- service.addClient(request.workerAddress, request.fileSize)
        // TODO: Map addClient
        result <- ZIO.succeed(WorkerDataResponse())
      } yield result

      result.mapError(e => new StatusException(Status.INTERNAL))
    }
  }
}

class MasterLogic(config: Config) {
  case class WorkerClient(val client: Layer[Throwable, WorkerServiceClient], val size: BigInt)
  
  var workerIPList: List[String] = List()
  var clients: List[WorkerClient] = List()

  lazy val offset: Long = clients.map(_.size).sum.toLong

  /** Add new client connection to MasterLogic
    *
    * @param clientAddress address of client
    */
  def addClient(workerAddress: String, workerSize: BigInt): IO[Throwable, Any] = {
    println(s"New worker[${clients.size}] attached: ${workerAddress}, Size: ${workerSize} Bytes")
    val address = AddressParser.parse(workerAddress).get
    clients = clients :+ WorkerClient(WorkerServiceClient.live(
      ZManagedChannel(
        ManagedChannelBuilder.forAddress(address._1, address._2).usePlaintext()
      )
    ), workerSize)
    workerIPList = workerIPList :+ workerAddress

    if (clients.size == config.workerNum.toOption.get) this.run()
    else ZIO.succeed(())
  }

  def run(): IO[Throwable, Any] = {
    println("All worker connected")

    for {
      _ <- zio.Console.printLine("Requests samples to each workers")
      pivotCandicateList <- ZIO.foreachPar(clients.map(_.client)) { layer =>
          collectSample(layer).provideLayer(layer)
        }
    } yield pivotCandicateList

    // ZIO는 실제로 사용되는 시점에서 비동기적으로 실행됨

    // val selectedPivots = selectPivots(pivotCandicateList)

    // clients.foreach(client => sendPartitionToWorker(client.client, selectedPivots))
  }
  
  def collectSample(client: Layer[Throwable, WorkerServiceClient]): ZIO[WorkerServiceClient, Throwable, Pivots] =
    ZIO.serviceWithZIO[WorkerServiceClient] { workerServiceClient =>
      workerServiceClient.getSamples(SampleRequest(offset))
  }

  // TODO: ZIO가 아니라 평범하게 List[Pivots]로 수정 요망. (코드 내부에서 비동기적으로 돌아가는 부분이 없으므로)
  def selectPivots(pivotCandicateZIOList: ZIO[Any, Throwable, List[Pivots]]): ZIO[Any, Throwable, Pivots] = {
    val pivotCandicateList: ZIO[Any, Throwable, List[String]] = pivotCandicateZIOList.map { pivots =>
      pivots.flatMap(_.pivots)
    }

    val pivotCandicateListSize: ZIO[Any, Throwable, BigInt] = pivotCandicateList.map(_.size)
    val totalDataSize: BigInt = clients.map(_.size).sum

    val pivotIndices: ZIO[Any, Throwable, List[Int]] = for {
      candidateListSize <- pivotCandicateListSize
      result <- ZIO.succeed(
        clients.map(_.size).scanLeft(0) { (acc, workerSize) =>
          acc + (candidateListSize * (workerSize / totalDataSize)).toInt
        }.tail
      )
    } yield result

    for {
      pivotList <- pivotCandicateList
      indices <- pivotIndices
      distinctList = indices.map(idx => pivotList(idx)).distinct
    } yield Pivots(pivots = distinctList)
  }

  def sendPartitionToWorker(client: Layer[Throwable, WorkerServiceClient],
                          pivots: ZIO[Any, Throwable, Pivots]): ZIO[Any, Throwable, Unit] = for {
  pivotsData <- pivots
  workerServiceClient <- ZIO.scoped(client.build).map(_.get) // ZEnvironment에서 WorkerServiceClient 추출
  shuffleRequest = ShuffleRequest(pivots = Some(pivotsData), workerAddresses = workerIPList)
  _ <- workerServiceClient.startShuffle(shuffleRequest).mapError(e => new RuntimeException(s"Failed to send ShuffleRequest: ${e.getMessage}"))
} yield ()

}
