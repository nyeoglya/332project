package master

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import Main._

@RunWith(classOf[JUnitRunner])
class MasterSuite extends FunSuite {
  
  DummyCommunicator.initWorker

  val worker1 = DummyCommunicator.getWorker("2.2.2.0:7777")
  val worker2 = DummyCommunicator.getWorker("2.2.2.1:7777")

  test("dummyWorker: sort") {
    assert(worker1.sortedData === List("01","02","03","04","05","06","07","08","09","10","13","15","21"))
  }

  test("getWorkerData: basic functionality test 1") {
    assert(getWorkerData("2.2.2.0:7777").isInstanceOf[WorkerData])
  }

  test("getWorkerData: basic functionality test 2") {
    assert(getWorkerData("2.2.2.0:7777").storageSize === worker1.storageSize)
  }

  test("getWorkerData: error handling test 1") {
    intercept[Exception] {
      getWorkerData("0.0.0.0:1233")
    }
  }

  test("getWorkerData: error handling test 2-1") {
    intercept[Exception] {
      getWorkerData("0.0.0:1233")
    }
  }

  test("getWorkerData: error handling test 2-2") {
    intercept[Exception] {
      getWorkerData("0.a.0.0:1233")
    }
  }

  test("getWorkerData: error handling test 2-3") {
    intercept[Exception] {
      getWorkerData("0.0.0.0")
    }
  }
}
