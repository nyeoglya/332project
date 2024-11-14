package worker

import org.scalatest.{ConfigMap, FunSuite}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.rogach.scallop._
import Main._
import common._
import java.io.{File, PrintWriter}
import scala.io.Source
import zio._
import zio.stream._
import zio.Runtime.default
import java.nio.file.Paths

// <1st test>
// gensort -a -b0 1000 input1.txt
// gensort -a -b1000 1000 input2.txt
// gensort -a -b2000 1000 input3.txt
// gensort -a -b3000 1000 input4.txt



@RunWith(classOf[JUnitRunner])
class WithoutNetworkSuite extends FunSuite {

  Mode.testMode = "WithoutNetworkTest"

  test("dummy test") {
    assert(true)
  }

  test("Main Config") {
    val args = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    Main.main(args)
    assert(config.masterAddress.toOption.get == "141.223.91.80:30040")
    assert(config.inputDirectories.toOption.get == List("src/test/withoutNetworkTestFiles/worker1/input"))
    assert(config.outputDirectory.toOption.get == "src/test/withoutNetworkTestFiles/worker1/output")
  }

  test("overall correctness") {
    val args1 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker1/input -O src/test/withoutNetworkTestFiles/worker1/output".split(' ')
    val args2 = "141.223.91.80:30040 -I src/test/withoutNetworkTestFiles/worker2/input -O src/test/withoutNetworkTestFiles/worker2/output".split(' ')

    val samples : ZIO[Any, Exception, Ref[List[Entity]]] = Ref.make(Nil)

    val program = for {
      fiber1 <- ZIO.attempt {
        Main.main(args1)
        Main.setMachineNumber(1)
        samples.map(_.update(_ ++ sampleStream))

      }.fork
      fiber2 <- ZIO.attempt {
        Main.main(args2)
        Main.setMachineNumber(2)
      }.fork
      _ <- fiber1.join
      _ <- fiber2.join
    } yield ()
  }
}
