package common

final case class Entity(head: String, body: String)

class Partition(pivots: List[String]) {
  def getEntityRangeIterator(): Iterator[EntityRange] = ???
}

case class EntityRange(left: String, right: String)
