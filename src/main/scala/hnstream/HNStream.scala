package hnstream

import java.util.concurrent.Semaphore
import com.firebase.client._
import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConverters._

object HNStream extends App {
  val fb = new Firebase("https://hacker-news.firebaseio.com/v0")

//  System.currentTimeMillis

  def pushUpdate(update: Update): Unit = {
    val bytes = update.toProtobufBytes
    println("push update " + bytes.length + " bytes")
  }

  val fbItems = fb.child("item")
  val objMapTypeInd = new GenericTypeIndicator[JMap[String, Object]](){}
  def fetchItem(itemId: Int) = {
    fbItems.child(itemId.toString).addListenerForSingleValueEvent(new ValueEventListener {
      override def onDataChange(dataSnapshot: DataSnapshot) = {
        val item = Item.fromSnapshot(dataSnapshot)
        val update = Update.createAtNow(item)
        pushUpdate(update)
        val serialized = update.toProtobufBytes
        println("item " + itemId + ": " + item + " (" + serialized.length + " bytes pbuf)")
        val deserialized = Update.fromProtobufBytes(serialized)
        println("des item " + deserialized.item.id)
      }
      override def onCancelled(firebaseError: FirebaseError) =
        println("error fetching item " + itemId + ": " + firebaseError)
    })
  }

  val fbUsers = fb.child("user")
  def fetchUser(userId: String) = {
    fbUsers.child(userId).addListenerForSingleValueEvent(new ValueEventListener {
      override def onDataChange(dataSnapshot: DataSnapshot) = {
        val user = User.fromSnapshot(dataSnapshot)
        val update = Update.createAtNow(user)
        val serialized = update.toProtobufBytes
        println("user " + userId + ": " + user + " (" + serialized.length + " bytes pbuf)")
        println("pre user " + user.id + " " + user.submitted.length)
        val deserialized = Update.fromProtobufBytes(serialized)
        println("des user " + deserialized.user.id + " " + deserialized.user.submitted.length)
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
        val itemUpdates = data.child("items").getValue(intListTypeInd)
        itemUpdates.asScala.foreach(notifyItemUpdate)
        val userUpdates = data.child("profiles").getValue(stringListTypeInd)
        userUpdates.asScala.foreach(notifyUserUpdate)
//        println( + " " + data.child("items").getValue(intListTypeInd))
//        data.child("profiles").getValue(stringListTypeInd).foreach(notifyUserUpdate)
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
