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

  def spec = suite("MasterSpec")(
    test("getSamplesFromWorker: basic functionality test case 1") {
    }
  )
}
