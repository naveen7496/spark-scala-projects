package part1recap

object ScalaRecap extends App {
  println("Hello world")
  val biggerno : String = if (3 > 6) "bigger" else "smaller"
//  println(biggerno)

  def myName(name:String): Unit ={
    println("my name is "+name)
  }
//
//  myName("NAveen")

  class Animal
  trait Carnivore{
    def eat(animal : Animal)
  }
  class Crocodile extends Animal with Carnivore{
    override def eat(animal: Animal): Unit = println("Crocodile eats wild Animals")
  }

//  val croc = new Crocodile()
//  croc.eat(new Animal())

  val incrementorr : Int => Double = x => x + 1

  val newList = List(1,2,3,4,5).map(incrementorr)
  println(newList)








}
