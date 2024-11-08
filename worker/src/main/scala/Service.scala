import common.Entity
import common.Partition

import zio.ZIO
import zio.stream.Stream

object PrototypeService {
  def getSampleStreamFromFile(data: Stream[Exception, Entity]): Stream[Exception, Entity] = ???

  def getPartialEntityStreamFromIndex(
    index: Int, 
    partition: Partition, 
    data: Stream[Exception, Entity]): Stream[Exception, Entity] = ???

  def getMergyStream(source: List[Stream[Exception, Entity]]): Stream[Exception, Entity] = ???
}
