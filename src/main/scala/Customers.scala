class Customers(val Uniq:Int,val timestamp:String,val CusId:Int) extends Serializable {

  override def toString():String =
  {
    "Uniq=" + Uniq + " " + "timestamp=" + timestamp + " " + "CustomerId="+ CusId
  }

}
