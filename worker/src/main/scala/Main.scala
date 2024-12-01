package worker

import org.rogach.scallop._
import zio._
import zio.stream._
import scalapb.zio_grpc.{ServerMain, ServiceList}

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter, PrintWriter}
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.io.Source
import io.grpc.StatusException
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import proto.common.{DataResponse, Entity, Pivots, SampleRequest, SortResponse}
import proto.common.ZioCommon.WorkerService
import scalapb.zio_grpc

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

import java.awt.image.DataBufferDouble
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import proto.common.DataRequest
import proto.common.ZioCommon.WorkerServiceClient


class Config(args: Seq[String]) extends ScallopConf(args) {
  val masterAddress = trailArg[String](required = true, descr = "Mater address (e.g. 192.168.0.1:8000)", default = Some("127.0.0.1"))
  val port = opt[Int](name = "P", descr = "Worker server port", default = Some(50051))
  val inputDirectories = opt[List[String]](name = "I", descr = "Input directories", default = Some(List()))
  val outputDirectory = opt[String](name = "O", descr = "Output directory", default = Some(""))
  verify()
}

object Main extends ZIOAppDefault {
  var port = 50051

  def run: ZIO[Environment with ZIOAppArgs with Scope,Any,Any] = { 
    (for {
      result <- serverLive.launch.exitCode.fork
      _ <- zio.Console.printLine(s"Worker is running on port ${port}. Press Ctrl-C to stop.")
      _ <- sendDataToMaster.mapError(e => {
        println("Error while send data to master")
      })
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
    result <- MasterServiceClient.sendWorkerData(WorkerData(worker.getFileSize, port))
  } yield result

  def sendDataToWorker(index: Int, has_next: Boolean, data: List[Entity]): ZIO[WorkerServiceClient, Throwable, DataResponse] = for {
    result <- WorkerServiceClient.sendData(DataRequest(index, has_next, data))
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
    }.catchAllCause { cause =>
      ZIO.fail {
        println(s"Get samples error cause : $cause")
        new StatusException(Status.INTERNAL)
      }
    }

    def startShuffle(request: ShuffleRequest): IO[StatusException,SortResponse] = {
      println(s"StartShuffle with index ${request.workerNumber} requested")
      service.workerNumber = request.workerNumber
      println("Worker addresses:")
      request.workerAddresses.foreach(println)
      service.totalWorker = request.workerAddresses.size
      val pivots = request.pivots.get
      println(s"Partition: ${pivots.pivots}")

      val files = service.getToWorkerNFilePaths(request.workerNumber, pivots)

      val result = for {
        result <- ZIO.foreach(files.zipWithIndex)({case (fileList, index) => {
          if (index != request.workerNumber) {
            println(s"Connect to: worker[${index}]")
            val layer = WorkerServiceClient.live(
                zio_grpc.ZManagedChannel(
                  ManagedChannelBuilder.forAddress(
                    request.workerAddresses(index).ip, 
                    request.workerAddresses(index).port
                  ).usePlaintext()
                )
            )
            layer.mapError(e => {
              println(s"Error while connecting worker[${index}]")
              println(e)
            })
            println(s"Connected to : worker[${index}]")
            for {
              result <- ZIO.foreach(fileList.zipWithIndex)(
              { case (filePath, index) => 
                {
                  val file = service.readFile(filePath)
                  println(s"Send File[${index}]")
                  for {
                    result <- sendDataToWorker(
                      request.workerNumber,
                      index != (fileList.size - 1),
                      file
                    ).provideLayer(layer).catchAllCause(cause => 
                     ZIO.fail{
                      println(s"Error while send file [${index}]: $cause")
                      new RuntimeException("SENDFILE")
                    })
                  } yield result
                }
              }).catchAllCause( cause => 
                ZIO.fail {
                  println(s"Send files error cause: $cause")
                  new RuntimeException("SENDFILES")
                }
              )
            } yield result
          } else ZIO.succeed(())
        }})
      } yield result
      result.catchAllCause(cause => 
        ZIO.succeed {
          println(s"Shuffle Request failed: $cause")
          SortResponse()
        }
        ).map(value => SortResponse())
    } 

    def sendData(request: DataRequest): IO[StatusException,DataResponse] = {
      println(s"Get data fregment from worker[${request.workerNumber}]")
      service.writeNetworkFile(request.payload.toList)
      if (!request.hasNext) {
        println(s"All data received from worker[${request.workerNumber}]")
        service.receivedWorker += 1
      }
      if (service.receivedWorker == service.totalWorker - 1) {
        println(s"Start merging files")
        service.mergeWrite(service.workerNumber)
        println(s"Complete Sorting")
      }
      ZIO.succeed(DataResponse())
    }.catchAllCause { cause => 
      ZIO.succeed {
        println(s"Send data Error cause: $cause")
        DataResponse()
      }
    }
  }
}

trait WorkerServiceLogic {
  var workerNumber = 0
  var receivedWorker = 0
  var totalWorker = 0

  /** Get whole file size of worker
    *
    * @return Int of bytes of file size
    */
  def getFileSize(): Long

  /** Get sample list
    * 
    * @return List of head of sample entity
    */
  def getSampleList(offset: Int): List[String]

  /** Get file paths List
   * n-th elem : file path List that will be sent to worker n
   *
   * @param partition
   * @return
   */
  def getToWorkerNFilePaths(workerNum: Int, partition: Pivots): List[List[String]]

  /** Merge given files using bottom up merge sort
   *
   * @param workerNum
   * @param partitionedFilePaths
   * @return
   */
  def mergeWrite(workerNum: Int) : String

  def readFile(filePath : String) : List[Entity]

  def writeNetworkFile(data: List[Entity]): Unit
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class WorkerLogic(config: Config) extends WorkerServiceLogic {
  /**
   * for test & comparing
   */
  val parallelMode = true

  /**
   * make newFile's path
   */
  private object PathMaker {
    def sortedSmallFile(fileNum: Int) : String = {
      config.outputDirectory.toOption.get + "/sorted_" + fileNum.toString
    }
    def sampleFile(filePath : String) : String = {
      val index = filePath.lastIndexOf('/')
      config.outputDirectory.toOption.get + "/sampled_" + filePath.substring(index + 1)
    }
    def partitionedFile(workerNum: Int, filePath: String): String ={
      val index = filePath.lastIndexOf('/')
      config.outputDirectory.toOption.get + "/to" + workerNum.toString + "_" + filePath.substring(index + 1)
    }
    def _3MBFile(num: Int, filePath: String): String = {
      val index = filePath.lastIndexOf('/')
      config.outputDirectory.toOption.get + "/s" + num.toString + "_" + filePath.substring(index + 1)
    }
    def shuffledFile(num: Int): String = {
      config.outputDirectory.toOption.get + "/shuffled_" + num.toString + (if(txtMode) ".txt" else "")
    }
    def mergedFile(num: Int, filePath: String): String = {
      val index = filePath.lastIndexOf('/')
      config.outputDirectory.toOption.get + "/m" + num.toString + "_" + filePath.substring(index + 1)
    }
    def resultFile(num: Int): String = {
      config.outputDirectory.toOption.get + "/merged" + num.toString + (if(txtMode) ".txt" else "")
    }
  }

  /** you can exploit parallelism using this function
   * achieve about 2 times higher overall performance
   *
   * @param target
   * @param func
   * @tparam A
   * @tparam B
   * @return
   */
  def useParallelism[A, B](target: List[A])(func: A => B): List[B] = {
    if(parallelMode) {
      val availableThreads = java.lang.Runtime.getRuntime.availableProcessors()
      val parallel: ZIO[Any, Throwable, List[B]] = {
        if(target.length <= availableThreads * 2)
          ZIO.foreachPar(target)(item => ZIO.succeed(func(item)))
        else
          ZIO.foreach(target.grouped(availableThreads).toList)(chunk => ZIO.foreachPar(chunk){item => ZIO.succeed(func(item))}).map(_.flatten)
      }
      Unsafe unsafe {implicit unsafe =>
        Runtime.default.unsafe.run(parallel).getOrThrow()
      }
    }
    else {
      target.map(func)
    }
  }

  /** read one entity
   *
   * @param reader
   * @return read entity (success) | Entity("","") (fail)
   */
  private def readEntity(reader: BufferedReader): Entity = {
    val line = reader.readLine()
    if(line == null) Entity("","")
    else Entity(line.splitAt(10)._1, line.splitAt(10)._2 + "\r\n")
  }

  def readFile(filePath : String) : List[Entity] = {
    val reader = new BufferedReader(new FileReader(filePath))
    val entities = Iterator.continually(readEntity(reader)).takeWhile(_ != Entity("","")).toList
    reader.close()
    entities
  }

  private def writeFile(filePath : String, data : List[Entity]) : Unit = {
    val writer = new BufferedWriter(new FileWriter(filePath))
    data.foreach(entity => {writer.write(entity.head); writer.write(entity.body)})
    writer.close()
  }

  def writeNetworkFile(data: List[Entity]): Unit = {
    val filePath = PathMaker.shuffledFile(shuffledFileCount)
    shuffledFileCount = shuffledFileCount + 1
    writeFile(filePath, data)
    shuffledFilePaths = filePath::shuffledFilePaths
  }

  /** sort given file in memory and save them on new file
   *
   * @param filePath
   * @return sorted file's path
   */
  private def sortSmallFile(filePath : String, fileNum: Int) : String = {
    //assert(Files.size(Paths.get(filePath)) < 50000000)
    val data = readFile(filePath)
    val sortedData = data.sortBy(entity => entity.head)
    val sortedFilePath = PathMaker.sortedSmallFile(fileNum)
    writeFile(sortedFilePath, sortedData)
    sortedFilePath
  }

  /** make sample file from given file by read with an given offset
   * after change structure
   * from   file -> List -
   * @param filePath
   * @param offset
   * @return sample file's path
   */
  def produceSampleFile(filePath : String, offset : Int) : String = {
    assert(offset > 0)
    val reader = new BufferedReader(new FileReader(filePath))
    val sampledFilePath = PathMaker.sampleFile(filePath)
    val writer = new BufferedWriter(new FileWriter(sampledFilePath))
    @tailrec
    def readOffsetWrite(count: Int): Unit = {
      val entity = readEntity(reader)
      if(entity == Entity("","")) ()
      else {
        if(count % offset == 0){
          writer.write(entity.head)
          writer.write(entity.body)
        }
        readOffsetWrite(count + 1)
      }
    }
    readOffsetWrite(0)
    reader.close()
    writer.close()
    sampledFilePath
  }

  /** split given file into N partitioned files using given pivots
   * after change function structure
   * from   file -> List -> files
   * to     file -> files
   * achieve about 2 times higher performance in partitioning
   *
   * @param filePath
   * @param pivots
   * @return list of partitioned file's path
   */
  def makePartitionedFiles(filePath : String, pivots : List[String]) : List[String] = {
    assert(pivots.nonEmpty)
    val reader = new BufferedReader(new FileReader(filePath))

    @tailrec
    def readPartitionWrite(acc: List[String], index: Int, writer: BufferedWriter, entity: Entity, count: Int): List[String] = {
      if(entity == Entity("","")) {
        writer.close()
        val oldFilePath =
          if(count != 0) PathMaker.partitionedFile(index, filePath) else ""
        acc ++ List(oldFilePath)
      }
      else if(index >= pivots.length || entity.head < pivots(index)) {
        writer.write(entity.head)
        writer.write(entity.body)
        readPartitionWrite(acc, index, writer, readEntity(reader), count + 1)
      }
      else {
        writer.close()
        val oldFilePath =
          if(count != 0) PathMaker.partitionedFile(index, filePath) else ""
        val newFilePath = PathMaker.partitionedFile(index + 1, filePath)
        val newAcc = acc ++ List(oldFilePath)
        val newWriter = new BufferedWriter(new FileWriter(newFilePath))
        readPartitionWrite(newAcc, index + 1, newWriter, entity, 0)
      }
    }
    val writer = new BufferedWriter(new FileWriter(PathMaker.partitionedFile(0, filePath)))
    val result = readPartitionWrite(List.empty[String], 0, writer, readEntity(reader), 0)
    reader.close()
    result.padTo(pivots.length + 1, "")
  }

  def mergeTwoFile(num: Int, path1: String, path2: String): String = {
    val reader1 = new BufferedReader(new FileReader(path1))
    val reader2 = new BufferedReader(new FileReader(path2))
    val filePath = PathMaker.mergedFile(num, path1)
    val writer = new BufferedWriter(new FileWriter(filePath))
    @tailrec
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
    //Files.delete(Paths.get(path1))
    //Files.delete(Paths.get(path2))
    filePath
  }

  private var totalFileSize: Long = 0

  private val originalSmallFilePaths : List[String] = {
    config.inputDirectories.toOption.getOrElse(List(""))
      .flatMap{ directoryPath =>
        val directory = new File(directoryPath)
        totalFileSize += directory.listFiles.filter(_.isFile).map(_.length).sum
        directory.listFiles.map(_.getPath.replace("\\", "/")).toList
      }
  }

  private val txtMode = originalSmallFilePaths.head.endsWith(".txt")

  val sortedSmallFilePaths: List[String] = useParallelism(originalSmallFilePaths.zipWithIndex){case (path, index) => sortSmallFile(path, index)}

  private var toNFilePaths: List[String] = List.empty[String]
  private var shuffledFileCount: Int = 0
  private var shuffledFilePaths: List[String] = List.empty[String]

  // TODO: Read files from storage
  def getFileSize(): Long = totalFileSize
  def getSampleList(offset: Int): List[String] = {
    val sampleFilePaths = useParallelism(sortedSmallFilePaths)(produceSampleFile(_, offset))
    val result = sampleFilePaths.flatMap(path => readFile(path)).map(entity => entity.head)
    //sampleFilePaths.foreach(path => Files.delete(Paths.get(path)))
    result
  }
  def getToWorkerNFilePaths(workerNum: Int, partition: Pivots): List[List[String]] = {
    def splitInto3MBFile(filePath: String): List[String] = {
      val fileSize = new File(filePath).length
      if(fileSize >= 4000000) {
        var count = 0
        val reader = new BufferedReader(new FileReader(filePath))
        var old_path = PathMaker._3MBFile(0, filePath)
        var writer = new BufferedWriter(new FileWriter(old_path))
        var resultFiles = List.empty[String]
        var entity = Entity("","")
        while({entity = readEntity(reader); entity != Entity("","")}) {
          writer.write(entity.head)
          writer.write(entity.body)
          count += 1
          if(count >= 30000) {
            count = 0
            resultFiles = old_path::resultFiles
            writer.close()
            old_path = PathMaker._3MBFile(resultFiles.length, filePath)
            writer = new BufferedWriter(new FileWriter(old_path))
          }
        }
        reader.close()
        if(count > 0) {
          resultFiles = old_path::resultFiles
        } else ()
        writer.close()
        resultFiles
      }
      else List(filePath)
    }
    val partitionFilePaths = useParallelism(sortedSmallFilePaths)(makePartitionedFiles(_, partition.pivots.toList))
    val toWorkerFilePaths = for {
      n <- (0 to partition.pivots.length).toList
      toN = partitionFilePaths.map(_(n))
    } yield toN.filter(_ != "").flatMap(path => splitInto3MBFile(path))
    //sortedSmallFilePaths.foreach(path => Files.delete(Paths.get(path)))
    shuffledFilePaths = toWorkerFilePaths(workerNum)
    toNFilePaths = toWorkerFilePaths.patch(workerNum, Nil, 1).flatten
    toWorkerFilePaths
  }

  def mergeWrite(workerNum: Int) : String = {
    @tailrec
    def mergeLevel(filePaths: List[String]): String = {
      if(filePaths.length == 1) filePaths.head
      else {
        val nextFilePaths =
          useParallelism(filePaths.sliding(2,2).toList.zipWithIndex){
            case (paths, index) =>
              if (paths.length == 1) paths.head
              else mergeTwoFile(index, paths(0), paths(1))
          }
        mergeLevel(nextFilePaths)
      }
    }
    val mergedFilePath = mergeLevel(shuffledFilePaths)
    val resultFilePath = PathMaker.resultFile(workerNum)
    Files.move(Paths.get(mergedFilePath), Paths.get(resultFilePath), StandardCopyOption.REPLACE_EXISTING)
    //toNFilePaths.foreach(path => Files.delete(Paths.get(path)))
    resultFilePath
  }
}



