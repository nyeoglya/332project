package master

import zio._
import zio.stream._
import java.time.Duration

// Dummy Worker Class. Removed Later.
trait DummyWorker {
  val workerIP: String
  val unsortedData: List[String]
  val delay: Duration = Duration.ofSeconds(2)

  lazy val sortedData: List[String] = {
    Thread.sleep(delay.toMillis)
    unsortedData.sorted
  }

  def storageSize: Int = sortedData.size

  def getSample: List[String] = sortedData.zipWithIndex.collect {
    case (value, index) if index % 3 == 0 => value
  }

  def getSampleStream: ZStream[Any, Nothing, String] = ZStream.fromIterable(sortedData)
    .zipWithIndex
    .filter { case (item, index) => index % 3 == 0 }
    .map { case (item, _) => item }
  
  def error = ???
}

// Dummy Communicator Class. Removed Later.
object DummyCommunicator {
  private var workerIPList: List[String] = Nil
  private var workerList: List[DummyWorker] = Nil
  
  lazy val initWorker: Unit = {
    val worker1 = new DummyWorker {
      val workerIP = "2.2.2.0:7777"
      val unsortedData = List("03","04","01","06","07","02","08","15","13","21","10","05","09")
    }

    val worker2 = new DummyWorker {
      val workerIP = "2.2.2.1:7777"
      val unsortedData = List("20","18","17","16","12","19","31")
    }

    DummyCommunicator.addWorker(worker1)
    DummyCommunicator.addWorker(worker2)
  }

  // 부수 효과는 없지만 var라서 테스트에만 써야할 듯
  def addWorker(worker: DummyWorker): Unit = {
    workerIPList = workerIPList :+ worker.workerIP
    workerList = workerList :+ worker
  }
  // 가정: 모든 workerIP는 구분가능하다.
  def getWorker(workerIP: String): DummyWorker = workerList.find(_.workerIP == workerIP) match {
    case Some(worker) => worker
    case _ => throw new Exception("failed to get worker")
  }
}