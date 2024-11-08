import common.Entity

import zio.ZIO
import zio.stream.ZStream

object Server {
  def getSampleStream(): ZStream[Any, Exception, Entity] = ???
  def startShuffling(): ZIO[Any, Exception, Unit] = ???
  def getPartialData(start: String, end: String): ZStream[Any, Exception, Entity] = ???
}
