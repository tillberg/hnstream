package hnstream

import java.util.{List => JList}
import com.firebase.client.DataSnapshot
import hnproto.PBMessages
import scala.collection.JavaConverters._

class User(_id: String,
           _delay: Int,
           _created: Int,
           _karma: Int,
           _about: String,
           _submitted: List[Int]) {
  val id = _id
  val delay = _delay
  val created = _created
  val karma = _karma
  val about = _about
  val submitted = _submitted

  def toProtobuf: PBMessages.User = {
    val builder = PBMessages.User.newBuilder
    builder.setId(id)
    builder.setDelay(delay)
    builder.setCreated(created)
    builder.setKarma(karma)
    builder.setAbout(about)
    submitted.foreach(builder.addSubmitted)
    builder.build()
  }

  def toProtobufBytes: Array[Byte] = {
    toProtobuf.toByteArray
  }
}

object User {
  def fromProtobuf(rec: PBMessages.User): User = {
    new User(
      rec.getId,
      rec.getDelay,
      rec.getCreated,
      rec.getKarma,
      rec.getAbout,
      rec.getSubmittedList.asScala.map(_.toInt).toList)
  }

  def fromProtobufBytes(serialized: Array[Byte]): User = {
    fromProtobuf(PBMessages.User.parseFrom(serialized))
  }

  def fromSnapshot(dataSnapshot: DataSnapshot) = {
    FirebaseConverters.extractUser(dataSnapshot)
  }
}
