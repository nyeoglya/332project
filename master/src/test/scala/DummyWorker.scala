package master

import zio._
import zio.test._
import zio.test.Assertion._
import zio.mock._
import zio.mock.environment._

import proto.common.Pivots
import proto.common.ZioCommon.WorkerServiceClient
import proto.common.SampleRequest

object WorkerServiceClientMock extends Mock[WorkerServiceClient] {
  object getSamples extends MockMethod[(SampleRequest), ZIO[WorkerServiceClient, Throwable, Pivots]] {
    def apply(req: SampleRequest): ZIO[WorkerServiceClient, Throwable, Pivots] = ???
  }

  val layer: ZLayer[Any, Nothing, WorkerServiceClient] =
    ZLayer.fromEffectTotal(new WorkerServiceClient {
      def getSamples(req: SampleRequest): ZIO[WorkerServiceClient, Throwable, Pivots] = getSamples(req)
    })
}
