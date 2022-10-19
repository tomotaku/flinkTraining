package org.data

class CustomUser() extends Serializable {
  var age:Int = _
  var name:String = _
  var id: Int = _
  def this(id: Int,name:String,age:Int){
    this()
    this.id = id
    this.name = name
    this.age = age
  }
  override def toString: String = Array(id,name,age).mkString(",")

}

