package worker

import org.rogach.scallop._
import scalapb.zio_grpc.{ServerMain, ServiceList}
import io.grpc.StatusException
import proto.common.{SampleRequest, SampleResponse}
import proto.common.ZioCommon.WorkerService
import zio.ZIOAppDefault
import zio.{Scope, ZIO, ZIOAppArgs}
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import zio.ZLayer
import scalapb.zio_grpc

class Config(args: Seq[String]) extends ScallopConf(args) {
  val inputDirectories = opt[List[String]](name = "I", descr = "Input directories", default = Some(List()))
  val outputDirectory = opt[String](name = "O", descr = "Output directory", default = Some(""))
  verify()
}

object Main extends ZIOAppDefault {
  def port: Int = 8980

  def run: ZIO[Environment with ZIOAppArgs with Scope,Any,Any] = for {
    args <- getArgs
    config = new Config(args)
    _ = config.inputDirectories.toOption.foreach(dirs => println(s"Input Directories: ${dirs.mkString(", ")}"))
    _ = config.outputDirectory.toOption.foreach(dir => println(s"Output Directory: $dir"))
    _ <- zio.Console.printLine(s"Worker is running on port ${port}. Press Ctrl-C to stop.")
    result <- serverLive.launch.exitCode
  } yield result

  def builder = ServerBuilder
    .forPort(port)
    .addService(ProtoReflectionService.newInstance())

  def serverLive: ZLayer[Any, Throwable, zio_grpc.Server] =
    zio_grpc.ServerLayer.fromServiceList(builder, ServiceList.add(ServiceImpl))
}

object ServiceImpl extends WorkerService {
  def getSamples(request: SampleRequest): zio.stream.Stream[StatusException,SampleResponse] = ???
}
