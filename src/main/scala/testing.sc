val t:Int = 7
val m= 5
println(t+m)
var s:String="123432"
s.substring(2,4)
s.length

trait Human{
  def twohands:String
  def twolegs:String
  def name:String
  def action:String ={"default action"}


  }

case class Man(i:String) extends Human {
  def twohands:String= { "Man two hands"}
  def twolegs:String= {"Man with two legs"}
  def name: String = {" Man with name" + i}
  override def action:String ={"Man action"}
}
case class Woman(i:String) extends Human {
  def twohands:String= { "Woman with two hands"}
  def twolegs:String= {"Woman with two legs"}
  def name: String = {" Woman with name" + i}
}

var obj = Man("Hasan")
val obj1 = Woman("Sarah")
obj.action
println(obj1.action)

obj=  Man("Hasan2")
val h:List[Human] =List(Man("Hasan"),Man("Hasan2"),Woman("Sarah"),Woman("Sarah2"))
//h.map(x =>println(x.name))
def funk(hum:Human):String = {
  hum.name + " " + hum.action
}
//h.filter(x=>x.isInstanceOf[Man]).map(p=>println(p.action))

  h.map(p=>println(funk(p)))

