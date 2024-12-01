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
import io.grpc.ServerInterceptor
import io.grpc.{Metadata, ServerCall, ServerCallHandler}
import io.grpc.ServerCall.Listener
import io.grpc.ForwardingClientCallListener
import io.grpc.ForwardingServerCallListener
import proto.common.SortResponse
import proto.common.Address
import java.lang

class Config(args: Seq[String]) extends ScallopConf(args) {
  val workerNum = trailArg[Int](required = true, descr = "Number of workers", default = Some(1))
  verify()
}

object Main extends ZIOAppDefault {
  def port: Int = 50050

  override def run: ZIO[Environment with ZIOAppArgs with Scope,Any,Any] = (for {
    _ <- zio.Console.printLine(s"Master is running on port ${port}")
    result <- serverLive.launch
  } yield result).provideSomeLayer[ZIOAppArgs](
    ZLayer.fromZIO( for {
        args <- getArgs
        config = new Config(args)
      } yield config
    ) >>> ZLayer.fromFunction {config: Config => new MasterLogic(config)}
  )

  def builder(logic: MasterLogic) = ServerBuilder
    .forPort(port)
    .intercept(new logic.WorkerIpInterceptor)
    .addService(ProtoReflectionService.newInstance())

  def serverLive: ZLayer[MasterLogic, Throwable, zio_grpc.Server] = {
    for {
      service <- ZLayer.service[MasterLogic]
      result <- zio_grpc.ServerLayer.fromServiceList(
        builder(service.get), 
        zio_grpc.ServiceList.add(new ServiceImpl(service.get)))
    } yield result
  }

  class ServiceImpl(service: MasterLogic) extends MasterService {
    def sendWorkerData(request: WorkerData): IO[StatusException,WorkerDataResponse] = {
      val result = for {
        addClient <- service.addClient(request.workerPort, request.fileSize)
        result <- ZIO.succeed(WorkerDataResponse())
      } yield result

      result.mapError(e => {
        e match {
          case e: StatusException => {
            println(e) 
            e
          } 
          case e => {
            println(e)
            new StatusException(Status.INTERNAL)
          }
        }
      })
    }.catchAllCause { cause => {
      ZIO.succeed {
        println(s"sendWorkerDataError cause: $cause")
        WorkerDataResponse()
      }
    }}
  }
}

class MasterLogic(config: Config) {
  case class WorkerClient(val client: Layer[Throwable, WorkerServiceClient], val size: Long)
  
  var workerIpQueue: List[String] = List()
  var workerIpList: List[Address] = List()
  var clients: List[WorkerClient] = List()

  lazy val offset: Int = List(1, (clients.map(_.size).sum / (1024 * 1024)).toInt).max

  // intercepts worker ip from grpc request
  class WorkerIpInterceptor extends ServerInterceptor {
    override def interceptCall[ReqT <: Object, RespT <: Object](
      call: ServerCall[ReqT,RespT], 
      headers: Metadata, 
      next: ServerCallHandler[ReqT,RespT]
    ): Listener[ReqT] = {
      val clientAddress = call.getAttributes.get(io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString.tail
      val clientIp = AddressParser.parse(clientAddress).get._1
      val methodName = call.getMethodDescriptor().getBareMethodName()
      if (methodName == "SendWorkerData") {
        workerIpQueue = workerIpQueue :+ clientIp
      }
      next.startCall(call, headers)
    }
  }

  /** Add new client connection to MasterLogic
    *
    * @param clientAddress address of client
    */
  def addClient(workerPort: Int, workerSize: Long): IO[Throwable, Any] = {
    assert { !workerIpQueue.isEmpty }
    if (clients.size >= config.workerNum.toOption.get) {
      println(s"Worker attached but rejected")
      ZIO.fail(new StatusException(Status.UNAVAILABLE))
    } else {
      val workerIp :: others = workerIpQueue
      workerIpQueue = others
      println(s"New worker[${clients.size}] attached: ${workerIp}:${workerPort}, Size: ${workerSize} Bytes")
      clients = clients :+ WorkerClient(WorkerServiceClient.live(
        ZManagedChannel(
          ManagedChannelBuilder.forAddress(workerIp, workerPort).usePlaintext()
        )
      ), workerSize)

      workerIpList = workerIpList :+ Address(workerIp, workerPort)

      if (clients.size == config.workerNum.toOption.get) this.run()
      else ZIO.succeed(())
    }
  }

  def run(): IO[Throwable, Any] = {
    println("All worker connected")

    for {
      _ <- zio.Console.printLine("Requests samples to each workers")
      pivotCandicateList <- ZIO.foreachPar(clients.map(_.client)) { layer =>
        collectSample.provideLayer(layer)
      }
      selectedPivots = selectPivots(pivotCandicateList)
      _ <- zio.Console.printLine(selectedPivots.pivots)
      _ <- zio.Console.printLine("Requests shuffle to each workers")
      result <- ZIO.foreachPar(clients.map(_.client).zipWithIndex) { 
        case (layer, index) => 
          sendPartition(index, selectedPivots).provideLayer(layer)
      }.catchAllCause { cause => {
        ZIO.fail {
          println(s"Send partition fail: $cause")
          new RuntimeException("SENDPARTITION")
        }
      }}
      _ <- zio.Console.printLine("Shffule request complete.")
    } yield result
  }.catchAllCause { cause => {
    ZIO.fail {
      println(s"Master run fail : $cause")
      new RuntimeException("MASTERRUN")
    }
  }}
  
  def collectSample: ZIO[WorkerServiceClient, Throwable, Pivots] =
    ZIO.serviceWithZIO[WorkerServiceClient] { workerServiceClient =>
      workerServiceClient.getSamples(SampleRequest(offset))
  }

  def sendPartition(number: Int, pivots: Pivots): ZIO[WorkerServiceClient, Throwable, SortResponse] =
    ZIO.serviceWithZIO[WorkerServiceClient] { workerServiceClient => 
      workerServiceClient.startShuffle(ShuffleRequest(pivots = Some(pivots), workerAddresses = workerIpList, workerNumber = number))
   }

  def selectPivots(pivotCandidateListOriginal: List[Pivots]): Pivots = {
    assert { !pivotCandidateListOriginal.isEmpty }
    assert { !clients.isEmpty }
    assert { pivotCandidateListOriginal.length == clients.length }

    val pivotCandidateList: List[String] = pivotCandidateListOriginal.flatMap(_.pivots).sorted

    val pivotCandidateListSize: Long = pivotCandidateList.size
    val totalDataSize: Long = clients.map(_.size).sum

    assert { totalDataSize != 0 }

    val clientSizes = clients.map(_.size)
    val pivotIndices: List[Int] = clientSizes.init.scanLeft(0) { (acc, workerSize) =>
      acc + (pivotCandidateListSize * (workerSize.toDouble / totalDataSize.toDouble)).toInt
    }.tail

    Pivots(pivotIndices.map(idx => pivotCandidateList(idx)))
  }
}
