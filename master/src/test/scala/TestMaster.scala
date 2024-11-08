package master

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import Main._

@RunWith(classOf[JUnitRunner])
class MasterSuite extends FunSuite {
  trait SimpleTest {
    //val s1 = dummyFunction(List(3,4,1))
  }

  test("dummyFunction test success") {
    //assert(dummyFunction(List(1,2,5,7)) === List(1,2))
  }

  test("dummyFunction test failed with message") {
    new SimpleTest {
      //assert(s1 === Nil, "Failture Message")
    }
  }
}
