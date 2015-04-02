package hnstream

import java.util.{Map => JMap, ArrayList}
import scala.collection.JavaConverters._
import com.firebase.client.{GenericTypeIndicator, DataSnapshot}

object FirebaseConverters {

  def getInt(_get: (String) => Any, k: String): Int = {
    val v = _get(k)
    v match {
      case n: Int => n
      case _ => 0
    }
  }
  def getString(_get: (String) => Any, k: String): String = {
    _get(k) match {
      case n: String => n
      case _ => ""
    }
  }
  def getBoolean(_get: (String) => Any, k: String): Boolean = {
    _get(k) match {
      case n: Boolean => n
      case _ => false
    }
  }
  def getIntList(_get: (String) => Any, k: String): List[Int] = {

    _get(k) match {
      case n: java.util.ArrayList[Integer] => n.asScala.toList.map(_.toInt)
      case _ => List[Int]()
    }
  }

  val objMapTypeInd = new GenericTypeIndicator[JMap[String, Object]](){}
  def getGetter(dataSnapshot: DataSnapshot): (String) => Any = {
    val data = dataSnapshot.getValue(objMapTypeInd)
    def get(k: String) = {
      data.get(k)
    }
    get
  }

  def extractItem(dataSnapshot: DataSnapshot): Item = {
    val get = getGetter(dataSnapshot)
    val id = getInt(get, "id")
    val deleted = getBoolean(get, "deleted")
    val _type = getString(get, "type")
    val by = getString(get, "by")
    val time = getInt(get, "time")
    val text = getString(get, "text")
    val dead = getBoolean(get, "dead")
    val parent = getInt(get, "parent")
    val kids = getIntList(get, "kids")
    val url = getString(get, "url")
    val score = getInt(get, "score")
    val title = getString(get, "title")
    val parts = getIntList(get, "parts")
    val descendants = getInt(get, "descendents")
    new Item(id, deleted, _type, by, time, text, dead, parent, kids, url, score, title, parts, descendants)
  }

  def extractUser(dataSnapshot: DataSnapshot): User = {
    val get = getGetter(dataSnapshot)
    val id = getString(get, "id")
    val delay = getInt(get, "delay")
    val created = getInt(get, "created")
    val karma = getInt(get, "karma")
    val about = getString(get, "about")
    val submitted = getIntList(get, "submitted")
    new User(id, delay, created, karma, about, submitted)
  }
}
