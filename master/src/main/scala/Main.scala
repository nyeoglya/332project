package master

object Main extends App {
  println("This is Master")

  def dummyFunction(elem: List[Int]): List[Int] = elem.filter(_ < 5)
}
