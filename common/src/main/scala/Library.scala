package common

import zio._
import java.text.ParseException
import com.google.protobuf.TextFormat

object AddressParser {
  def parse(address: String): Option[(String, Int)] = {
    address.split(":") match {
      case Array(ip, port) => Some(ip, port.toInt)
      case _ => None 
    }
  }
  
  def parseZIO(address: String): IO[Throwable, (String, Int)] = {
    address.split(":") match {
      case Array(ip, port) => ZIO.succeed((ip, port.toInt))
      case _ => ZIO.fail(new TextFormat.ParseException(s"Address parse failed: ${address}"))
    }
  }
}
