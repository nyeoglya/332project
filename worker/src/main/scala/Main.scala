package worker

import org.rogach.scallop._
import scalapb.zio_grpc.{ServerMain, ServiceList}
import io.grpc.StatusException
import proto.common.{SampleRequest, SampleResponse}
import proto.common.ZioCommon.WorkerService

class Config(args: Seq[String]) extends ScallopConf(args) {
  val masterAddress = trailArg[String](required = true, descr = "Master address (e.g., 192.168.0.1:8080)")
  val inputDirectories = opt[List[String]](name = "I", descr = "Input directories", default = Some(List()))
  val outputDirectory = opt[String](name = "O", descr = "Output directory", default = Some(""))
  verify()
}

object Main extends App {
  val config = new Config(args)
  println(s"Master Address: ${config.masterAddress()}")
  config.inputDirectories.toOption.foreach(dirs => println(s"Input Directories: ${dirs.mkString(", ")}"))
  config.outputDirectory.toOption.foreach(dir => println(s"Output Directory: $dir"))

  Server.run
}

object ServiceImpl extends WorkerService {
  def getSamples(request: SampleRequest): zio.stream.Stream[StatusException,SampleResponse] = ???
}

object Server extends ServerMain {
  def services = ServiceList.add(ServiceImpl)
  override def port: Int = 8980
}
