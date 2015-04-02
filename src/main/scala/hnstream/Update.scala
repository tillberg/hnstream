package hnstream

import hnproto.PBMessages

class Update(_obj: AnyRef, _time: Int) {
  val user = _obj match {
    case _user: User => _user
    case _ => null
  }
  val item = _obj match {
    case _item: Item => _item
    case _ => null
  }
  val toplist = _obj match {
    case _toplist: TopList => _toplist
    case _ => null
  }
  val time = _time

  def toProtobuf: PBMessages.Update = {
    val builder = PBMessages.Update.newBuilder
    builder.setTime(time)
    if (user != null) builder.setUser(user.toProtobuf)
    if (item != null) builder.setItem(item.toProtobuf)
    if (toplist != null) builder.setTopList(toplist.toProtobuf)
    builder.build
  }

  def toProtobufBytes: Array[Byte] = {
    toProtobuf.toByteArray
  }
}

object Update {
  def createAtNow(obj: AnyRef) = new Update(obj, (System.currentTimeMillis / 1000).toInt)

  def fromProtobuf(rec: PBMessages.Update): Update = {
    val obj = if (rec.hasUser) User.fromProtobuf(rec.getUser)
              else if (rec.hasItem) Item.fromProtobuf(rec.getItem)
              else TopList.fromProtobuf(rec.getTopList)
    new Update(obj, rec.getTime)
  }

  def fromProtobufBytes(serialized: Array[Byte]): Update = {
    fromProtobuf(PBMessages.Update.parseFrom(serialized))
  }
}
