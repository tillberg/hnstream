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
        if (item == null) {
          println("Could not fetch item " + itemId)
        } else {
          pushUpdate(Update.createAtNow(item))
        }
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
        if (user == null) {
          println("Could not fetch user " + userId)
        } else {
          pushUpdate(Update.createAtNow(user))
        }
      }
      override def onCancelled(firebaseError: FirebaseError) =
        println("error fetching user " + userId + ": " + firebaseError)
    })
  }

  def notifyUserUpdate(userId: String) = {
//    println("user " + userId + " updated")
    fetchUser(userId)
  }

  def notifyItemUpdate(itemId: Int) = {
    println("item " + itemId + " updated")
    fetchItem(itemId)
  }

  def now() = (System.currentTimeMillis / 1000).toInt

//  var prevMaxId = 0
  def notifyMaxItemId(newMaxId: Int) = {
    println(now + " max item id: " + newMaxId)
    // This is generally a wasted effort, as the items are not actually available to
    // fetch until an update event is emitted for the new items. Or something like that.
    // if (prevMaxId != 0) {
    //   while (prevMaxId < newMaxId) {
    //     prevMaxId = prevMaxId + 1
    //     println("fetching new item " + prevMaxId)
    //     fetchItem(prevMaxId)
    //   }
    // }
    // prevMaxId = newMaxId
  }

  def notifyUpdateTopList(listName: String, itemIds: List[Int]) = {
    val toplist = new TopList(listName, itemIds)
    val update = Update.createAtNow(toplist)
    pushUpdate(update)
    println(now + " updated " + listName + " (" + itemIds.size + " items), " + update.toProtobufBytes.length + " bytes")
  }

  val stringListTypeInd = new GenericTypeIndicator[JList[String]](){}
  val intListTypeInd = new GenericTypeIndicator[JList[Int]](){}
  val intTypeInd = new GenericTypeIndicator[Int](){}
  def handleDataChange(data: DataSnapshot) = {
    val key = data.getKey
    key match {
      case "updates" =>
        val itemUpdates = data.child("items").getValue(intListTypeInd)
        itemUpdates.asScala.foreach(notifyItemUpdate)
        val userUpdates = data.child("profiles").getValue(stringListTypeInd)
        userUpdates.asScala.foreach(notifyUserUpdate)
      case "maxitem" =>
        notifyMaxItemId(data.getValue(intTypeInd))
      case _ =>
        notifyUpdateTopList(key, data.getValue(intListTypeInd).asScala.toList)
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
