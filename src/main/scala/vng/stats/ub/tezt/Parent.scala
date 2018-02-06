package vng.stats.ub.tezt

class Parent(name: String) {
  
  private var name1: String = _
  
  def this() {
    
    this("Prent")
    this.name1 = "Dang Phuc Vinh"
    println(name1)
  }
}