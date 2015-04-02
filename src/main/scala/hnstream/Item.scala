package hnstream

import java.util.{List => JList}
import com.firebase.client.DataSnapshot
import scala.collection.JavaConverters._
import hnproto.PBMessages

class Item(_id: Int,
           _deleted: Boolean,
           __type: String,
           _by: String,
           _time: Int,
           _text: String,
           _dead:Boolean,
           _parent:Int,
           _kids: List[Int],
           _url: String,
           _score: Int,
           _title: String,
           _parts: List[Int],
           _descendants: Int) {
  val id = _id
  val deleted = _deleted
  val _type = __type
  val by = _by
  val time = _time
  val text = _text
  val dead = _dead
  val parent = _parent
  val kids = _kids
  val url = _url
  val score = _score
  val title = _title
  val parts = _parts
  val descendants = _descendants

  def toProtobuf: PBMessages.Item = {
    val builder = PBMessages.Item.newBuilder
    builder.setId(this.id)
    builder.setDeleted(this.deleted)
    builder.setType(this._type)
    builder.setBy(this.by)
    builder.setTime(this.time)
    builder.setText(this.text)
    builder.setDead(this.dead)
    builder.setParent(this.parent)
    kids.foreach(builder.addKids)
    builder.setUrl(this.url)
    builder.setScore(this.score)
    builder.setTitle(this.title)
    parts.foreach(builder.addParts)
    builder.setDescendents(this.descendants)
    builder.build()
  }

  def toProtobufBytes: Array[Byte] = {
    toProtobuf.toByteArray
  }
}

object Item {
  def fromProtobuf(rec: PBMessages.Item): Item = {
    new Item(
      rec.getId,
      rec.getDeleted,
      rec.getType,
      rec.getBy,
      rec.getTime,
      rec.getText,
      rec.getDead,
      rec.getParent,
      rec.getKidsList.asScala.map(_.toInt).toList,
      rec.getUrl,
      rec.getScore,
      rec.getTitle,
      rec.getPartsList.asScala.map(_.toInt).toList,
      rec.getDescendents)
  }

  def fromProtobufBytes(serialized: Array[Byte]): Item = {
    fromProtobuf(PBMessages.Item.parseFrom(serialized))
  }

  def fromSnapshot(dataSnapshot: DataSnapshot) = {
    FirebaseConverters.extractItem(dataSnapshot)
  }
}
