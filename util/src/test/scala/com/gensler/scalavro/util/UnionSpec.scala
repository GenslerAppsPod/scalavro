package com.gensler.scalavro.util

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import scala.reflect.runtime.universe._

import com.gensler.scalavro.util.Union.{union, prove}

class UnionSpec extends FlatSpec with ShouldMatchers {

  def typeSubsetOf(a: Seq[Type], b: Seq[Type]): Boolean = {
    a.foldLeft(true) { (result, tpe) =>
      result && b.exists { _ =:= tpe}
    }
  }

  "The union type helpers" should "allow one to define unions" in {

    type ISB = union [Int] #or [String] #or [Boolean]

    def unionFunction[T : prove [ISB] #containsType](t: T) {}

    unionFunction(55)
    unionFunction("hello")
    unionFunction(false)
    // unionFunction(math.Pi) // fails to compile (correct behavior)
  }

  it should "wrap union type definitions in a 'friendly' class" in {
    val wrapped = new Union[union [Int] #or [String]]

    wrapped.contains[Int] should be (true)
    wrapped.contains[String] should be (true)
    wrapped.contains[Double] should be (false)

    wrapped assign 55
    wrapped.value[Int] should be (Some(55))
    wrapped.value[String] should be (None)

    wrapped assign "hi, union!"
    wrapped.value[String] should be (Some("hi, union!"))
    wrapped.value[Int] should be (None)

    def unionFunction[T : wrapped.containsType] {}
    unionFunction[Int]
    unionFunction[String]
  }

  it should "know its member types" in {
    val wrapped = new Union[union [Int] #or [Double] #or [String] #or [Float]]
    val expectedMembers = Seq(typeOf[Int], typeOf[Double], typeOf[String], typeOf[Float])
    val actualMembers = wrapped.typeMembers

    // A subset B AND B subset A => A == B
    typeSubsetOf(expectedMembers, actualMembers) should be (true)
    typeSubsetOf(actualMembers, expectedMembers) should be (true)

  }

  it should "handle unary unions, no matter how silly that seems" in {
    val unary = new Union[union [Int] #apply]
    unary.contains[Int] should be (true)
    unary.contains[Boolean] should be (false)
  }

  it should "build up Union instances one type at a time" in {
    val unary = new Union[union [Int] #apply]
    val binary = new Union[unary.underlying #or [String]]
    val ternary = new Union[binary.underlying #or [Float]]

    import scala.language.existentials
    val t2 = Union.combine(binary.underlyingConjunctionTag, typeTag[Float])

    ternary.contains[Int] should be (true)
    ternary.contains[String] should be (true)
    ternary.contains[Float] should be (true)

    // A subset B AND B subset A => A == B
    typeSubsetOf(ternary.typeMembers, t2.typeMembers) should be (true)
    typeSubsetOf(t2.typeMembers, ternary.typeMembers) should be (true)
  }

}