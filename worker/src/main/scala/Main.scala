package worker

import org.rogach.scallop._
import zio._
import zio.stream._
import scalapb.zio_grpc.{ServerMain, ServiceList}
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.io.Source
import io.grpc.StatusException
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import proto.common.{SampleRequest, Entity, DataResponse, SortResponse, Pivots}
import proto.common.ZioCommon.WorkerService
import scalapb.zio_grpc
import java.nio.file.Files
import scala.collection.mutable.PriorityQueue
import scala.language.postfixOps
import proto.common.ShuffleRequest
import proto.common.ZioCommon.MasterServiceClient
import io.grpc.ManagedChannelBuilder
import proto.common.WorkerData
import proto.common.WorkerDataResponse
import common.AddressParser

class Config(args: Seq[String]) extends ScallopConf(args) {
  val masterAddress = trailArg[String](required = true, descr = "Mater address (e.g. 192.168.0.1:8000)", default = Some("127.0.0.1"))
  val port = opt[Int](name = "P", descr = "Worker server port", default = Some(8080))
  val inputDirectories = opt[List[String]](name = "I", descr = "Input directories", default = Some(List()))
  val outputDirectory = opt[String](name = "O", descr = "Output directory", default = Some(""))
  verify()
}

object Main extends ZIOAppDefault {
  var port = 8080

  def run: ZIO[Environment with ZIOAppArgs with Scope,Any,Any] = { 
    (for {
      _ <- sendDataToMaster
      result <- serverLive.launch.exitCode
    } yield result).provideSomeLayer[ZIOAppArgs](
      ZLayer.fromZIO( for {
        args <- getArgs
        config = new Config(args)
        _ <- zio.Console.printLine(s" Master Address: ${config.masterAddress.toOption.get}")
        _ = config.inputDirectories.toOption.foreach(dirs => println(s"Input Directories: ${dirs.mkString(", ")}"))
        _ = config.outputDirectory.toOption.foreach(dir => println(s"Output Directory: $dir"))
        _ <- zio.Console.printLine(s"Worker is running on port ${config.port.toOption.get}. Press Ctrl-C to stop.")
        _ = (port = {config.port.toOption.get})
      } yield config
    ) >>> ZLayer.fromFunction {config: Config => new WorkerLogic(config)} ++ masterClientLayer
  )}

  val masterClientLayer: ZLayer[Config, Throwable, MasterServiceClient] = for {
    config <- ZLayer.service[Config]
    address <- ZLayer.fromZIO(AddressParser.parseZIO(config.get.masterAddress.toOption.get))
    layer <- MasterServiceClient.live(
        zio_grpc.ZManagedChannel(
          ManagedChannelBuilder.forAddress(address.get._1, address.get._2).usePlaintext()
        )
    )
  } yield layer

  val sendDataToMaster: ZIO[MasterServiceClient with WorkerServiceLogic, Throwable, WorkerDataResponse] = for {
    worker <- ZIO.service[WorkerServiceLogic]
    result <- MasterServiceClient.sendWorkerData(WorkerData(worker.getFileSize, s"127.0.0.1:${port}"))
  } yield result

  def builder = ServerBuilder
    .forPort(port)
    .addService(ProtoReflectionService.newInstance())

  def serverLive: ZLayer[WorkerServiceLogic, Throwable, zio_grpc.Server] = for {
      service <- ZLayer.service[WorkerServiceLogic]
      result <- zio_grpc.ServerLayer.fromServiceList(builder, ServiceList.add(new ServiceImpl(service.get))) 
  } yield result

  class ServiceImpl(service: WorkerServiceLogic) extends WorkerService {
    def getSamples(request: SampleRequest): IO[StatusException,Pivots] = ???
    def startShuffle(request: ShuffleRequest): IO[StatusException,SortResponse] = ???
    def sendData(request: Stream[StatusException,Entity]): IO[StatusException,DataResponse] = ???
  }
}

trait WorkerServiceLogic {
  def inputEntities: Stream[Throwable, Entity]

  /** Get whole file size of worker
    *
    * @return Int of bytes of file size
    */
  def getFileSize(): Int

  /** Save Entities to file system
    * 
    *
    * @param data Stream of Entity
    */
  def saveEntities(index: Integer, data: Stream[Throwable, Entity])

  /** Get Stream of sample Entity datas
    * 
    * @return Stream of sample Entity to send
    */
  def getSampleStream(offset: Integer, size: Integer): List[String]

  /** Get Stream of Entity to send other workers
    *
    * @param index
    * @param partition
    * @return
    */
  def getDataStream(partition: Pivots): List[Stream[Throwable, Entity]]

  /** Sort multiple Entity Streams
    *
    * @param data
    * @return
    */
  def sortStreams(data: List[Stream[StatusException, Entity]]): Stream[Throwable, Entity]
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class WorkerLogic(config: Config) extends WorkerServiceLogic {

  object PathMaker {
    def sortedSmallFile(filePath : String) : String = {
      val index = filePath.lastIndexOf('/')
      config.outputDirectory.toOption.get + "/sorted_" + filePath.substring(index + 1)
    }
    def sampleFile(filePath : String) : String = {
      val index = filePath.lastIndexOf('/')
      config.outputDirectory.toOption.get + "/sampled_" + filePath.substring(index + 1)
    }
    def mergedFile(index : Integer) : String = {
      config.outputDirectory.toOption.get + "/mergedFile" + index.toString
    }
  }

  def zio2entity(zio : ZIO[Any, Throwable, Entity]) : Entity =
    Unsafe unsafe {
      implicit unsafe =>
        Runtime.default.unsafe.run(zio).getOrThrow()
    }

  def readFile(filePath : String) : List[Entity] = {
    val source = Source.fromFile(filePath)
    val lines = source.grouped(100).toList
    source.close()
    for {
      line <- lines
      (key, value) = line.splitAt(10)
    } yield Entity(key.mkString, value.mkString)
  }

  def writeFile(filePath : String, data : List[Entity]) : Unit = {
    val file = new File(filePath)
    val writer = new PrintWriter(file)
    try {
      data.foreach(entity => {
        writer.write(entity.head)
        writer.write(entity.body)
      })
    } finally {
      writer.close()
    }
  }

  def sortSmallFile(filePath : String) : String = {
    val data = readFile(filePath)
    val sortedData = data.sortBy(entity => entity.head)
    val sortedFilePath = PathMaker.sortedSmallFile(filePath)
    writeFile(sortedFilePath, sortedData)
    sortedFilePath
  }

  def produceSampleFile(filePath : String, stride : Integer) : String = {
    assert(stride > 0)
    val data = readFile(filePath)
    val sampledData = data.zipWithIndex.collect{ case (entity, index) if index % stride == 0 => entity}
    val sampledFilePath = PathMaker.sampleFile(filePath)
    writeFile(sampledFilePath, sampledData)
    sampledFilePath
  }

  def sampleFilesToSampleStream(filePaths : List[String]) : List[String] = {
    filePaths.flatMap(path => readFile(path)).map(entity => entity.head)
  }

  def splitFileIntoPartitionStreams(filePath : String, pivots : List[String]) : List[Stream[Throwable, Entity]] = {
    assert(pivots.nonEmpty)
    val data = readFile(filePath)
    def splitUsingPivots(data : List[Entity], pivots: List[String]) : List[List[Entity]] = pivots match {
      case Nil => List(data)
      case pivot::pivots =>
        data.takeWhile(entity => entity.head < pivot) :: splitUsingPivots(data.dropWhile(entity => entity.head < pivot), pivots)
    }
    splitUsingPivots(data, pivots).map(entityList => ZStream.fromIterable(entityList))
  }

  def mergeBeforeShuffle(partitionStreams : List[Stream[Throwable, Entity]]) : Stream[Throwable, Entity] = {
    implicit val entityOrdering : Ordering[(Entity, Int)] = Ordering.by(_._1.head)
    val minHeap : PriorityQueue[(Entity, Int)] = PriorityQueue.empty(entityOrdering.reverse)
    partitionStreams.zipWithIndex foreach {
      case (stream, index) =>
        val head = zio2entity(stream.runHead.map(_.getOrElse(Entity("", ""))))
        if(head != Entity("", ""))
          minHeap.enqueue((head, index))
    }
    def makeMergeStream(streams : List[Stream[Throwable, Entity]], accStream : Stream[Throwable, Entity]) : Stream[Throwable, Entity] = {
      if(minHeap.isEmpty) accStream
      else {
        val (minEntity, minIndex) = minHeap.dequeue()
        val head = zio2entity(streams(minIndex).runHead.map(_.getOrElse(Entity("", ""))))
        val tailStream = streams(minIndex).drop(1)
        if (head != Entity("", "")) minHeap.enqueue((head, minIndex))
        makeMergeStream(streams.updated(minIndex, tailStream), accStream ++ ZStream.succeed(minEntity))
      }
    }
    makeMergeStream(partitionStreams.map(_.drop(1)), ZStream.empty)
  }

  def mergeAfterShuffle(workerStreams : List[Stream[Throwable, Entity]]) : Stream[Throwable, Entity] = {
    mergeBeforeShuffle(workerStreams)
  }

  val originalSmallFilePaths : List[String] = {
    config.inputDirectories.toOption.getOrElse(List(""))
      .flatMap{ directoryPath =>
        val directory = new File(directoryPath)
        directory.listFiles.map(_.getPath.replace("\\", "/")).toList
      }
  }

  val sortedSmallFilePaths : List[String] = originalSmallFilePaths.map(path => sortSmallFile(path))

  // TODO: Read files from storage
  def inputEntities: Stream[Throwable, Entity] =
    ZStream.fromIterable(originalSmallFilePaths).flatMap(path => ZStream.fromIterable(readFile(path)))

  def saveEntities(index: Integer, data: Stream[Throwable,Entity]): Unit = {
    val file = new File(PathMaker.mergedFile(index))
    val writer = new PrintWriter(file)
    try {
      Unsafe unsafe {
        implicit unsafe =>
          Runtime.default.unsafe.run{
            data.runForeach(entity => {
              writer.write(entity.head)
              writer.write(entity.body)
              ZIO.succeed(())
            })
          }
      }
    } finally {
      writer.close()
    }
  }

  def getFileSize(): Int = originalSmallFilePaths.map(path => Files.size(Paths.get(path))).sum.toInt
  def getDataStream(partition: Pivots): List[Stream[Throwable,Entity]] = {
    val partitionStreams : List[List[Stream[Throwable, Entity]]] =
      sortedSmallFilePaths.map(path => splitFileIntoPartitionStreams(path, partition.pivots.toList))
    val toWorkerStreams = for {
      n <- (0 to partition.pivots.length).toList
      toN = partitionStreams.map(_(n))
    } yield toN
    toWorkerStreams.map(toWorkerN => mergeBeforeShuffle(toWorkerN))
  }
  def getSampleStream(offset: Integer, size: Integer): List[String] = {
    val sampleFilePaths = sortedSmallFilePaths.map(path => produceSampleFile(path, offset))
    sampleFilesToSampleStream(sampleFilePaths)
  }
  def sortStreams(data: List[Stream[StatusException,Entity]]): Stream[Throwable,Entity] = mergeAfterShuffle(data)
}

/** Worker's Main function
 *
 *  모드에 따라 파일의 양식, path 위치가 다르기에, 편의를 위해 [[readFile]], [[writeFile]], [[PathMaker]] 를 정의해 사용하였다.
 *
 *  주요 함수들은 아래와 같다.
 *
 *  [[sortSmallFile]]
 *  [[produceSampleFile]]
 *  [[sampleFilesToSampleStream]]
 *  [[splitFileIntoPartitionStreams]]
 *  [[mergeBeforeShuffle]]
 *  [[mergeAfterShuffle]]
 *
 *  각각은 그저 일반 함수처럼 동작한다.
 *  위의 함수들을 병렬적으로 돌리고, 기다리는 작업은 밑의 "real workflow begin from here" 부분에서 시행한다.
 *
 *  - [ ] TODO #1 : 병렬화
 *
 *  - [ ] TODO #2 : 네트워크 서비스와의 연결
 *
 *  - [x] TODO #3 : 중간 파일 지우기
 *
 *  - [x] TODO #4 : sample 파일 key 로만 구성
 *
 *  - [ ] TODO #5 : error handling 추가
 *
 */
/*object WorkerCodes extends App {
  val config = new Config(args)

  config.inputDirectories.toOption.foreach(dirs => println(s"Input Directories: ${dirs.mkString(", ")}"))
  config.outputDirectory.toOption.foreach(dir => println(s"Output Directory: $dir"))


  /** read file from filePath and return its data in a list of Entity
   *  depending on mode, it works differently
   *
   * @param filePath path of the file to read
   * @return data of the file read
   */
  def readFile(filePath : String) : List[Entity] = {
    val source = Source.fromFile(filePath)
    val lines = source.grouped(100).toList
    source.close()
    for {
      line <- lines
      (key, value) = line.splitAt(10)
    } yield Entity(key.mkString, value.mkString)
  }

  /** write data to new file
   *  depending on mode, it works differently
   *
   * @param filePath path of the file to write
   * @param data a list of entity to write
   */
  def writeFile(filePath : String, data : List[Entity]) : Unit = {
    val file = new File(filePath)
    val writer = new PrintWriter(file)
    try {
      data.foreach(entity => {
        writer.write(entity.head)
        writer.write(entity.body)
      })
    } finally {
      writer.close()
    }
  }

  /** write data to new file through stream
   *  depending on mode, it works differently
   *
   *  because size of file is limited when we write file in a normal way,
   *  we introduce stream to write huge entities on one single file
   *
   * @param filePath path of the file to write
   * @param data a list of entity to write(huge)
   */
  def writeFileViaStream(filePath : String, data : Stream[Exception, Entity]) : Unit = {
    val file = new File(filePath)
    val writer = new PrintWriter(file)
    try {
      Unsafe unsafe {
        implicit unsafe =>
          Runtime.default.unsafe.run{
            data.runForeach(entity => {
              writer.write(entity.head)
              writer.write(entity.body)
              ZIO.succeed(())
            })
          }
      }
    } finally {
      writer.close()
    }
  }

  /** this function literally sort small File
   *  and save sorted data in somewhere and return that path
   *
   * @param filePath original file's path
   * @return sorted file's path
   */
  def sortSmallFile(filePath : String) : String = {
    val data = readFile(filePath)
    val sortedData = data.sortBy(entity => entity.head)
    val sortedFilePath = PathMaker.sortedSmallFile(filePath)
    writeFile(sortedFilePath, sortedData)
    sortedFilePath
  }

  /** this function sample data from given file
   *  by striding @stride entities
   *
   * @param filePath given file's path(sorted)
   * @param stride
   * @return new sample file's path
   */
  def produceSampleFile(filePath : String, stride : Int) : String = {
    assert(stride > 0)
    val data = readFile(filePath)
    val sampledData = data.zipWithIndex.collect{ case (entity, index) if index % stride == 0 => entity}
    val sampledFilePath = PathMaker.sampleFile(filePath)
    writeFile(sampledFilePath, sampledData)
    sampledFilePath
  }

  /** this function merge given sample files
   *  and return a single merged sample data(in list)
   *  here, 'merge' does not consider ordering. it just collects them
   *
   * @param filePaths list of given sample file
   * @return merged sample data
   */
  def sampleFilesToSampleStream(filePaths : List[String]) : List[String] = {
    filePaths.flatMap(path => readFile(path)).map(entity => entity.head)
  }

  /** this function split given file into several mini streams using pivots
   *
   * @param filePath target file's path
   * @param pivots list of pivot
   * @return list of mini streams
   */
  def splitFileIntoPartitionStreams(filePath : String, pivots : List[String]) : List[Stream[Exception, Entity]] = {
    assert(pivots.nonEmpty)
    val data = readFile(filePath)
    def splitUsingPivots(data : List[Entity], pivots: List[String]) : List[List[Entity]] = pivots match {
      case Nil => List(data)
      case pivot::pivots =>
        data.takeWhile(entity => entity.head < pivot) :: splitUsingPivots(data.dropWhile(entity => entity.head < pivot), pivots)
    }
    splitUsingPivots(data, pivots).map(entityList => ZStream.fromIterable(entityList))
  }

  /** this function extract entity from ZIO value
   *
   * @param zio original ZIO value
   * @return entity that had been in ZIO
   */
  def zio2entity(zio : ZIO[Any, Exception, Entity]) : Entity =
    Unsafe unsafe {
      implicit unsafe =>
        Runtime.default.unsafe.run(zio).getOrThrow()
    }

  /** this function merge list of streams into a single sorted stream
   *  all streams in input want to go same worker so we'll make them one
   *  using priorityQueue, we can efficiently merge streams.
   *  to access entity in ZStream, we use zio2entity function
   *
   * @param partitionStreams list of partitioned streams
   * @return a merged stream
   */
  def mergeBeforeShuffle(partitionStreams : List[Stream[Exception, Entity]]) : Stream[Exception, Entity] = {
    implicit val entityOrdering : Ordering[(Entity, Int)] = Ordering.by(_._1.head)
    val minHeap : PriorityQueue[(Entity, Int)] = PriorityQueue.empty(entityOrdering.reverse)
    partitionStreams.zipWithIndex foreach {
      case (stream, index) =>
        val head = zio2entity(stream.runHead.map(_.getOrElse(Entity("", ""))))
        if(head != Entity("", ""))
          minHeap.enqueue((head, index))
    }
    def makeMergeStream(streams : List[Stream[Exception, Entity]], accStream : Stream[Exception, Entity]) : Stream[Exception, Entity] = {
      if(minHeap.isEmpty) accStream
      else {
        val (minEntity, minIndex) = minHeap.dequeue()
        val head = zio2entity(streams(minIndex).runHead.map(_.getOrElse(Entity("", ""))))
        val tailStream = streams(minIndex).drop(1)
        if (head != Entity("", "")) minHeap.enqueue((head, minIndex))
        makeMergeStream(streams.updated(minIndex, tailStream), accStream ++ ZStream.succeed(minEntity))
      }
    }
    makeMergeStream(partitionStreams.map(_.drop(1)), ZStream.empty)
  }

  /** this function merge list of streams into a single sorted stream(same with mergeBeforeShuffle)
   *  and write this huge stream into single file and return its path
   *
   * @param workerStreams list of streams that sent by other workers
   * @return merged file's path
   */
  def mergeAfterShuffle(workerStreams : List[Stream[Exception, Entity]]) : String = {
    val mergedStream = mergeBeforeShuffle(workerStreams)
    val filePath = PathMaker.mergedFile()
    writeFileViaStream(filePath, mergedStream)
    filePath
  }

  val machineNumber : Int = 1
  val numberOfFiles : Int = 1
  val pivotList :List[String] = List("")
  val workerIpList : List[String] = List("")
  val sampleOffset : Int = 100

  /// real workflow begin from here //////////////////////////////////////////////////////////////////////////////////

  val originalSmallFilePaths : List[String] = {
    config.inputDirectories.toOption.getOrElse(List(""))
      .flatMap{ directoryPath =>
        val directory = new File(directoryPath)
        directory.listFiles.map(_.getPath.replace("\\", "/")).toList
      }
  }
  // send data to master and wait 'getSample' command from master
  /**
   * TODO : make it parallel
   */
  val sortedSmallFilePaths : List[String] = originalSmallFilePaths.map(path => sortSmallFile(path))

  /**
   * TODO : make it parallel
   */
  val sampleFilePaths : List[String] = sortedSmallFilePaths.map(path => produceSampleFile(path, sampleOffset))

  val sampleStream : List[String] = sampleFilesToSampleStream(sampleFilePaths)

  sampleFilePaths.foreach(path => new File(path).delete())

  // send sampleStream and wait pivotList
  // after receive pivotList, send stream request to other workers

  /**
   * TODO : make it parallel
   */
  val partitionStreams : List[List[Stream[Exception, Entity]]] =
    sortedSmallFilePaths.map(path => splitFileIntoPartitionStreams(path, pivotList))

  val toWorkerStreams = for {
    n <- (0 to pivotList.length).toList
    toN = partitionStreams.map(_(n))
  } yield toN


  /**
   * TODO : make it parallel
   */
  val beforeShuffleStreams : List[Stream[Exception, Entity]] =
    toWorkerStreams.map(toWorkerN => mergeBeforeShuffle(toWorkerN))

  // wait until receive all stream from other workers
  val shuffledStreams = List(ZStream.succeed(Entity("","")))


  val mergedFilePath : String = mergeAfterShuffle(shuffledStreams)

  sortedSmallFilePaths.foreach(path => new File(path).delete())
}*/
