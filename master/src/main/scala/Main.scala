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

  def addClient(clientAddress: String): Boolean = {
    clients = clients :+ WorkerServiceClient.live(
      ZManagedChannel(
        ManagedChannelBuilder.forAddress(clientAddress, 8080).usePlaintext()
      )
    )
    
    return (clients.size == config.workerNum.toOption.get)
  }

  def clientLayers: List[Layer[Throwable,WorkerServiceClient]] = {
    ???
  }
  def collectSamples(): List[Stream[Throwable,Entity]] = ???
  def selectPivots(samples: List[Stream[Throwable,Entity]]): Pivots = ???
}

