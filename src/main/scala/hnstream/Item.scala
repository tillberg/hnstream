package hnstream

import java.io.ByteArrayOutputStream
import java.util.{List => JList, Map => JMap}
import com.firebase.client.{GenericTypeIndicator, DataSnapshot}
import scala.collection.JavaConversions._

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
           _descendants: JList[Int]) {
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

  def toProtobufMsg(): Array[Byte] = {

//    rec.put("id", this.id)
//    rec.put("deleted", this.deleted)
//    rec.put("_type", this._type)
//    rec.put("by", this.by)
//    rec.put("time", this.time)
//    rec.put("text", this.text)
//    rec.put("dead", this.dead)
//    rec.put("parent", this.parent)
//    rec.put("kids", this.kids)
//    rec.put("url", this.url)
//    rec.put("score", this.score)
//    rec.put("title", this.title)
//    rec.put("parts", this.parts)
//    rec.put("descendants", this.descendants)
    Array[Byte]()
  }
}

object Item {

  def generateFromGetter(_get: (String) => Any) = {
    def getInt(k: String): Int = {
      val v = _get(k)
      v match {
        case n: java.lang.Integer => n.intValue
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
    val descendants = getIntArray("descendents")
    new Item(id, deleted, _type, by, time, text, dead, parent, kids, url, score, title, parts, descendants)
  }

  def fromProtobuf(serialized: Array[Byte]) = {
//    new Item(rec.id, rec.deleted, rec._type, rec.by, rec.time, rec.text, rec.dead, rec.parent, rec.kids, rec.url, rec.score, rec.title, rec.parts, rec.descendants)
  }

  val objMapTypeInd = new GenericTypeIndicator[JMap[String, Object]](){}
  def fromSnapshot(dataSnapshot: DataSnapshot) = {
    val data = dataSnapshot.getValue(objMapTypeInd)
    def get(k: String) = {
      data.get(k)
    }
    generateFromGetter(get)
  }
}
