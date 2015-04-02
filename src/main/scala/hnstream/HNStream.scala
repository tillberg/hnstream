package hnstream

import java.util.concurrent.Semaphore
import com.firebase.client._
import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConversions._

object HNStream extends App {
  val fb = new Firebase("https://hacker-news.firebaseio.com/v0")

//  System.currentTimeMillis

  val fbItems = fb.child("item")
  val objMapTypeInd = new GenericTypeIndicator[JMap[String, Object]](){}
  def fetchItem(itemId: Int) = {
    fbItems.child(itemId.toString).addListenerForSingleValueEvent(new ValueEventListener {
      override def onDataChange(dataSnapshot: DataSnapshot) = {
        val item = Item.fromSnapshot(dataSnapshot)
        val serialized = item.toProtobufMsg
        println("item " + itemId + ": " + item + " (" + serialized.length + " bytes pbuf)")
        println(serialized.map("%02X" format _).mkString)
        val deserialized = Item.fromProtobuf(serialized)
        println(deserialized.id)
      }
      override def onCancelled(firebaseError: FirebaseError) =
        println("error fetching item " + itemId + ": " + firebaseError)
    })
  }

  val fbUsers = fb.child("user")
  def fetchUser(userId: String) = {
    fbUsers.child(userId).addListenerForSingleValueEvent(new ValueEventListener {
      override def onDataChange(dataSnapshot: DataSnapshot) = {
        val user = dataSnapshot.getValue(objMapTypeInd)
        println("user " + userId + ": " + user)
      }
      override def onCancelled(firebaseError: FirebaseError) =
        println("error fetching user " + userId + ": " + firebaseError)
    })
  }

  def notifyUserUpdate(userId: String) = {
    println("user " + userId + " updated")
    fetchUser(userId)
  }

  def notifyItemUpdate(itemId: Int) = {
    println("item " + itemId + " updated")
    fetchItem(itemId)
  }

  def now() = (System.currentTimeMillis / 1000).toInt

  def notifyMaxItemId(itemId: Int) = {
    println(now + " max item id: " + itemId)
  }

  def notifyUpdateTopList(listName: String, itemIds: JList[Int]) = {
    println(now + " updated " + listName + " (" + itemIds.size + " items)")
  }

  val stringListTypeInd = new GenericTypeIndicator[JList[String]](){}
  val intListTypeInd = new GenericTypeIndicator[JList[Int]](){}
  val intTypeInd = new GenericTypeIndicator[Int](){}
  def handleDataChange(data: DataSnapshot) = {
    val key = data.getKey
    key match {
      case "updates" =>
        println(now + " updates")
//        println( + " " + data.child("items").getValue(intListTypeInd))
//        data.child("profiles").getValue(stringListTypeInd).foreach(notifyUserUpdate)
        data.child("items").getValue(intListTypeInd).foreach(notifyItemUpdate)
      case "maxitem" =>
        notifyMaxItemId(data.getValue(intTypeInd))
      case _ =>
        notifyUpdateTopList(key, data.getValue(intListTypeInd))
    }
  }

  val topics = Array("updates", "maxitem", "topstories", "askstories", "showstories", "jobstories")
  def listenTopic(topic: String) = {
    fb.child(topic).addValueEventListener(new ValueEventListener {
      override def onDataChange(dataSnapshot: DataSnapshot) = handleDataChange(dataSnapshot)

      override def onCancelled(firebaseError: FirebaseError) =
        println("error watching " + topic + ": " + firebaseError)
    })
  }
  topics.map(listenTopic)

  new Semaphore(0).acquire()
}
