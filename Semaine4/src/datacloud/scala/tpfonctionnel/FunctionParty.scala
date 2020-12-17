package datacloud.scala.tpfonctionnel

object FunctionParty {

  def curryfie[A, B, C](f: (A, B) => C): A => B => C = (a: A) => (b: B) => f(a, b)

  def decurryfie[A, B, C](f: A => B => C): (A, B) => C = (a: A, b: B) => f(a)(b)

  def compose[A, B, C](f: B => C, g: A => B): A => C = (a: A) => f(g(a))

  def axplusb(a: Int, b: Int): Int => Int = curryfie((a: Int, b: Int) => compose((ax: Int) => ax + b, (x: Int) => a * x))(a)(b)

}