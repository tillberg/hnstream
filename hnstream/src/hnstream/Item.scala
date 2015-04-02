package hnstream

import java.util.{List => JList, Map => JMap}
import com.firebase.client.{DataSnapshot, GenericTypeIndicator}
import scala.collection.JavaConversions._
import hnproto.PBMessages

class Item(_id: Int,
           _deleted: Boolean,
           __type: String,
           _by: String,
           _time: Int,
           _text: String,
           _dead:Boolean,
           _parent:Int,
           _kids: JList[Int],
           _url: String,
           _score: Int,
           _title: String,
           _parts: JList[Int],
           _descendants: Int) {
  var id = _id
  var deleted = _deleted
  var _type = __type
  var by = _by
  var time = _time
  var text = _text
  var dead = _dead
  var parent = _parent
  var kids = _kids
  var url = _url
  var score = _score
  var title = _title
  var parts = _parts
  var descendants = _descendants

  def toProtobufMsg: Array[Byte] = {
    val builder = PBMessages.Item //.newBuilder
//    builder.setId(this.id)
//    builder.setDeleted(this.deleted)
//    builder.setType(this._type)
//    builder.setBy(this.by)
//    builder.setTime(this.time)
//    builder.setText(this.text)
//    builder.setDead(this.dead)
//    builder.setParent(this.parent)
//    this.kids.foreach(builder.addKids)
//    builder.setUrl(this.url)
//    builder.setScore(this.score)
//    builder.setTitle(this.title)
//    this.parts.foreach(builder.addParts)
//    builder.setDescendents(this.descendants)
//    builder.build().toByteArray
     Array[Byte]()
  }
}

object Item {

  def generateFromGetter(_get: (String) => Any) = {
    def getInt(k: String): Int = {
      val v = _get(k)
      v match {
        case n: Int => n
        case _ => 0
      }
    }
    def getString(k: String): String = {
      _get(k) match {
        case n: String => n
        case _ => ""
      }
    }
    def getBoolean(k: String): Boolean = {
      _get(k) match {
        case n: Boolean => n
        case _ => false
      }
    }
    def getIntArray(k: String): JList[Int] = {

      _get(k) match {
        case n: Array[Int] => n.toList
        case _ => List[Int]()
      }
    }
    val id = getInt("id")
    val deleted = getBoolean("deleted")
    val _type = getString("type")
    val by = getString("by")
    val time = getInt("time")
    val text = getString("text")
    val dead = getBoolean("dead")
    val parent = getInt("parent")
    val kids = getIntArray("kids")
    val url = getString("url")
    val score = getInt("score")
    val title = getString("title")
    val parts = getIntArray("parts")
    val descendants = getInt("descendents")
    new Item(id, deleted, _type, by, time, text, dead, parent, kids, url, score, title, parts, descendants)
  }

//  def fromProtobuf(serialized: Array[Byte]): Item = {
//    val rec = PBMessages.Item.parseFrom(serialized)
//    new Item(
//      rec.getId,
//      rec.getDeleted,
//      rec.getType,
//      rec.getBy,
//      rec.getTime,
//      rec.getText,
//      rec.getDead,
//      rec.getParent,
//      rec.getKidsList.map(_.toInt),
//      rec.getUrl,
//      rec.getScore,
//      rec.getTitle,
//      rec.getPartsList.map(_.toInt),
//      rec.getDescendents)
//  }

  val objMapTypeInd = new GenericTypeIndicator[JMap[String, Object]](){}
  def fromSnapshot(dataSnapshot: DataSnapshot) = {
    val data = dataSnapshot.getValue(objMapTypeInd)
    def get(k: String) = {
      data.get(k)
    }
    generateFromGetter(get)
  }
}
