package master

import org.rogach.scallop._

class Config(args: Seq[String]) extends ScallopConf(args) {
  val workerNum = trailArg[Int](required = true, descr = "Number of workers")
  verify()
}

object Main extends App {
  val config = new Config(args)
  println(s"Number of Workers: ${config.workerNum()}")
}
