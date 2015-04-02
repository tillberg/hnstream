package hnstream

import hnproto.PBMessages
import scala.collection.JavaConverters._

class TopList(_title: String,
              _ids: List[Int]) {
  val title = _title
  val ids = _ids

  def toProtobuf: PBMessages.TopList = {
    val builder = PBMessages.TopList.newBuilder
    builder.setTitle(title)
    ids.foreach(builder.addIds)
    builder.build()
  }

  def toProtobufBytes: Array[Byte] = {
    toProtobuf.toByteArray
  }
}

object TopList {
  def fromProtobuf(rec: PBMessages.TopList): TopList = {
    new TopList(
      rec.getTitle,
      rec.getIdsList.asScala.map(_.toInt).toList)
  }

  def fromProtobufBytes(serialized: Array[Byte]): TopList = {
    fromProtobuf(PBMessages.TopList.parseFrom(serialized))
  }
}
