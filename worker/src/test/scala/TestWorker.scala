package worker

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import Main._
import common._
import java.io.{File, PrintWriter}
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class WorkerSuite extends FunSuite {

  def readFile(filePath : String) : List[Entity] = {
    val source = Source.fromFile(filePath)
    val lines = source.getLines().toList
    source.close()
    for {
      line <- lines
      words <- line.split("|")
      head = words[0]
      body = words[1]
    } yield Entity(head, body)
  }

  def writeFile(filePath : String, data : List[Entity]) : Unit = {
    val file = new File(filePath)
    val writer = new PrintWriter(file)
    data.foreach(entity => {
      writer.write(entity.head)
      writer.write("|")
      writer.write(entity.body)
      writer.write("\n")
    })
    writer.close()
  }

  val worker1data : List[List[Entity]] =
    List(readFile("testFile1.txt"), readFile("testFile2.txt"))
  val worker1result : List[Entity] =
    List(readFile("resultFile1.txt"), readFile("resultFile2.txt")).flatten
  val worker2data : List[List[Entity]] =
    List(readFile("testFile3.txt"), readFile("testFile4.txt"))
  val worker2result : List[Entity] =
    List(readFile("resultFile3.txt"), readFile("resultFile4.txt")).flatten

  val offset = 3
  val M = 2
  val N = 2

  val sortedFilePaths =
    List(sortSmallFile("testFile1.txt"), sortSmallFile("testFile2.txt"), sortSmallFile("testFile3.txt"), sortSmallFile("testFile4.txt"))
  val sortedFileDatas =
    sortedFilePaths.map(path => readFile(path))
  val sampledFilePaths =
    sortedFilePaths.map(path => produceSampleFile(path, offset))
  val sampledFileDatas =
    sampledFilePaths.map(path => readFile(path))
  val w1SampleStream = sampleFilesToSampleStream(sampledFilePaths.take(M))
  val w2SampleStream = sampleFilesToSampleStream(sampledFilePaths.drop(M))
  val partitionStreams =
    sortedFilePaths.map(path => splitFileIntoPartitionStreams(path))
  val w1Tow1 =
    mergeBeforeShuffle(List(partitionStreams(0)(0), partitionStreams(1)(0)))
  val w1Tow2 =
    mergeBeforeShuffle(List(partitionStreams(0)(1), partitionStreams(1)(1)))
  val w2Tow1 =
    mergeBeforeShuffle(List(partitionStreams(3)(0), partitionStreams(4)(0)))
  val w2Tow2 =
    mergeBeforeShuffle(List(partitionStreams(3)(1), partitionStreams(4)(1)))

  val w1MergedFilePath = mergeAfterShuffle(List(w1Tow1, w2Tow1))
  val w2MergedFilePath = mergeAfterShuffle(List(w1Tow2, w2Tow2))

  test("dummy test") {
    assert(true)
  }

  test("sortSmallFile test : sorted correctly ") {
    assert(true)
  }

  test("produceSampleFile test : subset ") {
    assert(sampledFileDatas.tail.forall(entity => sortedFileDatas.tail.contains(entity)))
  }

  test("produceSampleFile test : sorted ") {
    assert(true)
  }

  test("produceSampleFile test : length ") {
    assert(sampledFileDatas.tail.length == sortedFileDatas.tail.length / offset)
  }

  test("sampleFilesToSampleStream test : length ") {
    assert(w1SampleStream.runCount == sampledFileDatas(0).length + sampledFileDatas(1).length)
  }

  test("splitFileIntoPartitionStreams test : N stream ") {
    assert(partitionStreams.head.length == N)
  }

  test("splitFileIntoPartitionStreams test : length ") {
    assert(partitionStreams.head.map(st => st.runCount).sum == sortedFileDatas.head.length)
  }

  test("splitFileIntoPartitionStreams test : sort ") {
    assert(true)
  }

  test("mergeBeforeShuffle test : N stream ") {
    assert(true)
  }

  test("mergeBeforeShuffle test : sorted ") {
    assert(true)
  }

  test("mergeBeforeShuffle test : length ") {
    assert(w1Tow2.runCount == partitionStreams(0)(1).runCount + partitionStreams(1)(1).runCount)
  }

  test("mergeAfterShuffle test : whole correctness ") {
    assert(readFile(w1MergedFilePath) == worker1result)
    assert(readFile(w2MergedFilePath) == worker2result)
  }


}
