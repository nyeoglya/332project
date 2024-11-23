package worker

import org.rogach.scallop._
import zio._
import zio.stream._
import scalapb.zio_grpc.{ServerMain, ServiceList}

import java.io.{BufferedReader, File, FileReader, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.io.Source
import io.grpc.StatusException
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import proto.common.{DataResponse, Entity, Pivots, SampleRequest, SortResponse}
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
import zio.stream.ZStream.{HaltStrategy, fromQueue}
import io.grpc.Status

import java.nio.charset.StandardCharsets
import scala.annotation.tailrec


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
      result <- serverLive.launch.exitCode.fork
      _ <- zio.Console.printLine(s"Worker is running on port ${port}. Press Ctrl-C to stop.")
      _ <- sendDataToMaster
      result <- result.join
    } yield result)
      .provideSomeLayer[ZIOAppArgs](
        ZLayer.fromZIO( 
          for {
            args <- getArgs
            config = new Config(args)
            _ <- zio.Console.printLine(s" Master Address: ${config.masterAddress.toOption.get}")
            _ = config.inputDirectories.toOption.foreach(dirs => println(s"Input Directories: ${dirs.mkString(", ")}"))
            _ = config.outputDirectory.toOption.foreach(dir => println(s"Output Directory: $dir"))
            _ = (port = {config.port.toOption.get})
          } yield config
        ) 
      >>> ZLayer.fromFunction {config: Config => new WorkerLogic(config)} 
      ++ masterClientLayer
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
    def getSamples(request: SampleRequest): IO[StatusException,Pivots] = {
      val result = for {
        _ <- zio.Console.printLine(s"Sample requested with offset: ${request.offset}")
        samples = service.getSampleList(request.offset.toInt)
        result = Pivots(samples)
      } yield result
      result.mapError(e => new StatusException(Status.INTERNAL))
    }

    def startShuffle(request: ShuffleRequest): IO[StatusException,SortResponse] = ???
    def sendData(request: Stream[StatusException,Entity]): IO[StatusException,DataResponse] = ??? }
}

trait WorkerServiceLogic {
  def inputEntities: Stream[Throwable, Entity]

  /** Get whole file size of worker
    *
    * @return Int of bytes of file size
    */
  def getFileSize(): Long

  /** Get Stream of sample Entity datas
    * 
    * @return Stream of sample Entity to send
    */
  def getSampleList(offset: Int): List[String]

  /** Get file paths List
   * n-th elem : file path List that will be sent to worker n
   *
   * @param partition
   * @return
   */
  def getToWorkerNFilePaths(partition: Pivots): List[List[String]]

  /** Merge given files using heap sort
   * when one minValue determined by heap sort, immediately write it on result File
   *
   * @param workerNum
   * @param partitionedFilePaths
   * @return
   */
  def mergeWrite(workerNum: Int, partitionedFilePaths : List[String]) : String
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class WorkerLogic(config: Config) extends WorkerServiceLogic {

  /**
   * make newFile's path
   */
  object PathMaker {
    def sortedSmallFile(filePath : String) : String = {
      val index = filePath.lastIndexOf('/')
      config.outputDirectory.toOption.get + "/sorted_" + filePath.substring(index + 1)
    }
    def sampleFile(filePath : String) : String = {
      val index = filePath.lastIndexOf('/')
      config.outputDirectory.toOption.get + "/sampled_" + filePath.substring(index + 1)
    }
    def partitionedFile(workerNum: Int, filePath: String) ={
      val index = filePath.lastIndexOf('/')
      config.outputDirectory.toOption.get + "/to" + workerNum.toString + "_" + filePath.substring(index + 1)
    }
    def mergedFile(num: Int, filePath: String): String = {
      val index = filePath.lastIndexOf('/')
      config.outputDirectory.toOption.get + "/m" + num.toString + "_" + filePath.substring(index + 1)
    }
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

  def produceSampleFile(filePath : String, stride : Int) : String = {
    assert(stride > 0)
    val data = readFile(filePath)
    val sampledData = data.zipWithIndex.collect{ case (entity, index) if index % stride == 0 => entity}
    val sampledFilePath = PathMaker.sampleFile(filePath)
    writeFile(sampledFilePath, sampledData)
    sampledFilePath
  }

  def sampleFilesToSampleList(filePaths : List[String]) : List[String] = {
    filePaths.flatMap(path => readFile(path)).map(entity => entity.head)
  }

  def makePartitionedFiles(filePath : String, pivots : List[String]) : List[String] = {
    assert(pivots.nonEmpty)
    val data = readFile(filePath)

    def splitUsingPivots(data: List[Entity], pivots: List[String]): List[List[Entity]] = pivots match {
      case Nil => List(data)
      case pivot :: pivots =>
        data.takeWhile(entity => entity.head < pivot) :: splitUsingPivots(data.dropWhile(entity => entity.head < pivot), pivots)
    }

    splitUsingPivots(data, pivots).zipWithIndex.map { case (entityList, index) =>
      val partitionedFilePath = PathMaker.partitionedFile(index, filePath)
      writeFile(partitionedFilePath, entityList)
      partitionedFilePath
    }
  }

  def readEntity(reader: BufferedReader): Entity = {
    val line = reader.readLine()
    if(line == null) Entity("","")
    else Entity(line.splitAt(10)._1, line.splitAt(10)._2 + java.lang.System.lineSeparator())
  }
  def mergeTwoFile(num: Int, path1: String, path2: String): String = {
    val reader1 = new BufferedReader(new FileReader(path1))
    val reader2 = new BufferedReader(new FileReader(path2))
    val filePath = PathMaker.mergedFile(num, path1)
    val writer = new PrintWriter(filePath)
    def merge(entity1: Entity, entity2: Entity): Unit = {
      if(entity1 == Entity("","") && entity2 == Entity("","")) ()
      else if(entity1.head < entity2.head && entity1 != Entity("","") || entity2 == Entity("","")) {
        writer.write(entity1.head)
        writer.write(entity1.body)
        merge(readEntity(reader1), entity2)
      }
      else {
        writer.write(entity2.head)
        writer.write(entity2.body)
        merge(entity1, readEntity(reader2))
      }
    }
    merge(readEntity(reader1), readEntity(reader2))
    writer.close()
    reader1.close()
    reader2.close()
    Files.delete(Paths.get(path1))
    Files.delete(Paths.get(path2))
    filePath
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

  def getFileSize(): Long = originalSmallFilePaths.map(path => Files.size(Paths.get(path))).sum.toLong
  def getSampleList(offset: Int): List[String] = {
    val parallelSampling: ZIO[Any, Throwable, List[String]] =
      ZIO.foreachPar(sortedSmallFilePaths)(path => ZIO.succeed(produceSampleFile(path, offset)))
    val sampleFilePaths = Unsafe unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(parallelSampling).getOrThrow()
    }
    sampleFilesToSampleList(sampleFilePaths)
  }
  def getToWorkerNFilePaths(partition: Pivots): List[List[String]] = {
    val parallelPartitioning: ZIO[Any, Throwable, List[List[String]]] =
      ZIO.foreachPar(sortedSmallFilePaths)(path => ZIO.succeed(makePartitionedFiles(path, partition.pivots.toList)))
    val partitionFilePaths : List[List[String]] =
      Unsafe unsafe { implicit unsafe =>
        Runtime.default.unsafe.run(parallelPartitioning).getOrThrow()
      }
    val toWorkerFilePaths = for {
      n <- (0 to partition.pivots.length).toList
      toN = partitionFilePaths.map(_(n))
    } yield toN
    toWorkerFilePaths
  }

  def mergeWrite(workerNum: Int, partitionedFilePaths : List[String]) : String = {
    def mergeLevel(filePaths: List[String]): String = {
      if(filePaths.length == 1) filePaths.head
      else {
        val parallelMerging: ZIO[Any, Throwable, List[String]] =
          ZIO.foreachPar(filePaths.sliding(2,2).toList.zipWithIndex) {
            case (paths, index) =>
              if (paths.length == 1) ZIO.succeed(paths.head)
              else ZIO.succeed(mergeTwoFile(index, paths(0), paths(1)))
          }
        val nextFilePaths =
          Unsafe unsafe { implicit unsafe =>
            Runtime.default.unsafe.run(parallelMerging).getOrThrow()
          }
        mergeLevel(nextFilePaths)
      }
    }
    mergeLevel(partitionedFilePaths)
  }
}
