import common.Entity
import common.Partition

import zio.ZIO
import zio.stream.Stream

object PrototypeService {
  def getPartitionData(sources: Stream[Exception, Entity]): Partition = ???
}
