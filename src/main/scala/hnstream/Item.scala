package hnstream

import java.io.ByteArrayOutputStream
import java.util.{List => JList, Map => JMap}
import com.firebase.client.{GenericTypeIndicator, DataSnapshot}
import com.julianpeeters.avro.annotations._
import org.apache.avro.file.{DataFileWriter, SeekableByteArrayInput, DataFileReader}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader}

@AvroTypeProvider("/Users/dtillberg/work/hnstream/data/item.avsc")
case class ItemRecord()

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
           _descendants: List[Int]) {
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
    def getIntArray(k: String): List[Int] = {
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

  def fromAvroMsg(serialized: Array[Byte]) = {
    val sin = new SeekableByteArrayInput(serialized)
    val datumReader = new SpecificDatumReader[ItemRecord]()
    val dataFileReader = new DataFileReader(sin, datumReader)
    val rec = dataFileReader.next()
    new Item(rec.id, rec.deleted, rec._type, rec.by, rec.time, rec.text, rec.dead, rec.parent, rec.kids, rec.url, rec.score, rec.title, rec.parts, rec.descendants)
  }

  def toAvroMsg(item: Item): Array[Byte] = {
    var rec = new ItemRecord()
    rec.id = item.id
    rec.deleted = item.deleted
    rec._type = item._type
    rec.by = item.by
    rec.time = item.time
    rec.text = item.text
    rec.dead = item.dead
    rec.parent = item.parent
    rec.kids = item.kids
    rec.url = item.url
    rec.score = item.score
    rec.title = item.title
    rec.parts = item.parts
    rec.descendants = item.descendants
    val baos = new ByteArrayOutputStream
    val datumWriter = new SpecificDatumWriter[ItemRecord]()
    val dataFileWriter = new DataFileWriter(datumWriter)
    dataFileWriter.create(ItemRecord.getSchema, baos)
    dataFileWriter.append(rec)


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
