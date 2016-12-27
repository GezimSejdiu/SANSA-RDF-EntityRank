package net.sansa_stack.entityrank.flink.utils

object Utils {

  /* Calculates 'x' modulo 'mod', takes to consideration sign of x.
  */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

}