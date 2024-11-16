package master

import org.junit.runner.RunWith

import zio._
import zio.stream._
import zio.test.{test, _}
import zio.test.Assertion._
import zio.test.junit.JUnitRunnableSpec
import zio.test.junit.ZTestJUnitRunner

import Main._

@RunWith(classOf[ZTestJUnitRunner])
class MasterSpec extends ZIOSpecDefault {

  DummyCommunicator.initWorker

  val worker1 = DummyCommunicator.getWorker("2.2.2.0:7777")
  val worker2 = DummyCommunicator.getWorker("2.2.2.1:7777")

  def spec = suite("MasterSpec")(
    test("getSamplesFromWorker: basic functionality test case 1") {
      val sampleStream = getSamplesFromWorker(workerIPList.head) // worker.getSampleStream
      val expected = worker1.getSample // List(1, 4, 7, 10, 21)

      for {
        result <- sampleStream.runCollect.map(_.toList)
      } yield assert(result)(equalTo(expected))
    },
    test("getSamplesFromWorker: basic functionality test case 2") {
      val sampleStream = getSamplesFromWorker("")
      val expected = Nil

      for {
        result <- sampleStream.runCollect.map(_.toList)
      } yield assert(result)(equalTo(expected))
    },
    test("selectPivots: basic functionality test case 1") {
      val stream1 = worker1.getSampleStream
      val stream2 = worker2.getSampleStream
      val sampleStreamList: List[ZStream[Any,Nothing,String]] = List(stream1, stream2)
      val expected = List(???)

      val pivots = selectPivots(sampleStreamList)

      assert(pivots)(equalTo(expected))
    },
    test("selectPivots: basic functionality test case 2") {
      val sampleStreamList: List[ZStream[Any,Nothing,String]] = List(worker1.getSampleStream)
      val expected = List(???)

      val pivots = selectPivots(sampleStreamList)

      assert(pivots)(equalTo(expected))
    }
    /*,
    test("selectPivots: error handling test case 1") {
      
    },
    test("selectPivots: error handling test case 2") {
      
    },
    test("selectPivots: boundary test 1") {

    },
    test("sendPivots: basic functionality test case 1") {
      
    },
    test("sendPivots: error handling test case 1") {
      
    },
    test("sendPivots: error handling test case 2") {
      
    }*/
  )
}
