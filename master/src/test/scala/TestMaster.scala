package master

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import proto.common.Pivots

import Main._

@RunWith(classOf[JUnitRunner])
class MasterSuite extends FunSuite {

  def initMasterLogic(workerKeyList: List[Seq[Int]]): MasterLogic = {
    val workerCount: Int = workerKeyList.length
    val config: Config = new Config(Seq(workerCount.toString))
    val masterLogic: MasterLogic = new MasterLogic(config)
    
    // 테스트에서는 workerSize에 데이터 개수 넣어줘야 함
    for {
      i <- 0 until workerCount
    } {
      masterLogic.addClient(s"127.0.0.${i+1}:7777", workerKeyList(i).length.toLong)
    }
    
    masterLogic
  }

  def convertToString(dataList: List[Seq[Int]]): List[Seq[String]] = {
    dataList.map(seq => seq.map(_.toString))
  }

  def convertToPivots(dataList: List[Seq[Int]]): List[Pivots] = {
    dataList.map(seq => Pivots(seq.map(_.toString)))
  }

  test("selectPivots: basic functionality test 1") {
    val dataList = List(Seq(1,2), Seq(3))
    val masterLogic = initMasterLogic(dataList)

    assert(masterLogic.selectPivots(convertToPivots(dataList)) == Pivots(Seq("3")))
  }

  test("selectPivots: basic functionality test 2") {
    val dataList = List(Seq(1,2,3,4,5,6,7))
    val masterLogic = initMasterLogic(dataList)
    
    assert(masterLogic.selectPivots(convertToPivots(dataList)) == Pivots(Seq()))
  }
}
