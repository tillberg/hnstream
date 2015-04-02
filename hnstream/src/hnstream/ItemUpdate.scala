package hnstream

class ItemUpdate(_item: Item, _timestamp: Long) {
  val item = _item
  val timestamp = _timestamp
}

object ItemUpdate {
  def fromItemNow(item: Item) = new ItemUpdate(item, System.currentTimeMillis)
}
