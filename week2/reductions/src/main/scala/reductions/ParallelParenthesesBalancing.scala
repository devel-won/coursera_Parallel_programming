package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var parLeft, parRight = 0
    for (char <- chars) {
      char match {
        case '(' => parLeft += 1
        case ')' => parRight += 1
        case _ => None
      }
      if (parLeft < parRight)
        return false
    }
    if (parLeft == parRight)
      true
    else
      false
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): Int = {
      if (idx == until)
        return arg1 - arg2
      chars(idx) match {
        case '(' => traverse(idx + 1, until, arg1 + 1, arg2)
        case ')' => traverse(idx + 1, until, arg1, arg2 + 1)
        case _ => traverse(idx + 1, until, arg1, arg2)
      }

    }

    def reduce(from: Int, until: Int): Int = {
      if (until - from <= threshold)
        traverse(from, until, 0, 0)
      else {
        val mid = (from + until) / 2
        val (par1, par2) = parallel(reduce(from, mid), reduce(mid, until))
        par1 + par2
      }
    }

    reduce(0, chars.length) == 0
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
