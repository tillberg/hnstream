package hnstream

import java.io.ByteArrayOutputStream
import java.util.{List => JList, Map => JMap}
import com.firebase.client.{GenericTypeIndicator, DataSnapshot}
import com.julianpeeters.avro.annotations._
import org.apache.avro.file.{DataFileWriter, SeekableByteArrayInput, DataFileReader}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader}
import org.apache.avro.Schema
import scala.collection.JavaConversions._

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

  def fromAvroMsg(serialized: Array[Byte]) = {
    val sin = new SeekableByteArrayInput(serialized)
    val datumReader = new SpecificDatumReader[ItemRecord]()
    val dataFileReader = new DataFileReader(sin, datumReader)
    val rec = dataFileReader.next()
    new Item(rec.id, rec.deleted, rec._type, rec.by, rec.time, rec.text, rec.dead, rec.parent, rec.kids, rec.url, rec.score, rec.title, rec.parts, rec.descendants)
  }

  def toAvroMsg(item: Item): Array[Byte] = {
    val schemaStr = scala.io.Source.fromFile("data/item.avsc").mkString
    val parser = new Schema.Parser
    val schema = parser.parse(schemaStr)
    val rec = new GenericData.Record(schema)
    rec.put("id", item.id)
    rec.put("deleted", item.deleted)
    rec.put("_type", item._type)
    rec.put("by", item.by)
    rec.put("time", item.time)
    rec.put("text", item.text)
    rec.put("dead", item.dead)
    rec.put("parent", item.parent)
    rec.put("kids", item.kids)
    rec.put("url", item.url)
    rec.put("score", item.score)
    rec.put("title", item.title)
    rec.put("parts", item.parts)
    rec.put("descendants", item.descendants)
    val baos = new ByteArrayOutputStream
    val datumWriter = new SpecificDatumWriter[GenericData.Record]()
    val dataFileWriter = new DataFileWriter(datumWriter)
    dataFileWriter.create(schema, baos)
    dataFileWriter.append(rec)
    dataFileWriter.close()
    baos.close()
    baos.toByteArray
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
