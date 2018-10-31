
import is.hail.utils._

import java.util.Arrays

import scala.language.implicitConversions

class A(val i: Int)

object A {
  implicit def int2A(i: Int) = new A(i)
  implicit def ord: Ordering[A] = Ordering.by(_.i)
}

object Bench {
  def main(args: Array[String]): Unit = {
    val listSize = args(0).toInt
    val listLength = args(1).toInt
    val maxInt = args(2).toInt
    println(s"parameters: listSize=${listSize} listLength=${listLength} maxInt=${maxInt}")
    println("creating arrays")
    val r = scala.util.Random
    val arrays: Array[Array[A]] = Array.fill(listSize)(null)
    var i = 0; while (i < arrays.size) {
      val ar = new Array[A](listLength)
      var j = 0; while (j < ar.size) {
        ar(j) = r.nextInt(maxInt)
        j += 1
      }
      arrays(i) = ar
      i += 1
    }
    println("arrays created")
    println("sorting arrays")
    val sorted = arrays.map(_.sorted)
    println("arrays sorted")
    println()
    println("iterative")
    println("iteration begin")
    val tstart = System.nanoTime()
    val it = FlipbookIterator.multiZipJoin(sorted.map(_.toIterator.toFlipbookIterator), null, A.ord.compare)
    while (it.isValid) {
      it.advance()
    }
    val tend = System.nanoTime()
    println(s"iteration end: ${(tend - tstart) / 1e9} s")
    println()
    println("priority queue")
    val pstart = System.nanoTime()
    println("iteration begin")
    val pq = FlipbookIterator.multiZipJoinPq(sorted.map(_.toIterator.toFlipbookIterator), A.ord.compare)
    while (pq.isValid) {
      pq.advance()
    }
    val pend = System.nanoTime()
    println(s"iteration end: ${(pend - pstart) / 1e9} s")
  }

  def fixup(size: Int, buf: ArrayBuilder[(A, Int)]): Array[A] = { // simulate rvb for priority queue
    val ar: Array[A] = Array.fill(size)(null)
    var i = 0; while (i < buf.length) {
      ar(buf(i)._2) = buf(i)._1
      i += 1
    }
    ar
  }
}
