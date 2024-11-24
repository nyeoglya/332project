package master

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import proto.common.Pivots

import Main._

@RunWith(classOf[JUnitRunner])
class MasterSuite extends FunSuite {

  val config = new Config(Seq("3"))
  val masterLogic = new MasterLogic(config)

  test("getWorkerData: basic functionality test 1") {
    assert(masterLogic.selectPivots(List(Pivots(pivots=Seq("1","2","3","4","5")))) == Pivots(Seq()))
  }

  test("selectPivots: error handling test 1") {
    intercept[Exception] {

    }
  }
}
