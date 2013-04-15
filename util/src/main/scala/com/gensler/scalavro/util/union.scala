package com.gensler.scalavro
package util

import scala.reflect.runtime.universe._

/**
  * Provides a facility for specifying unboxed union types of arbitrary
  * ordinality in the Scala type system.
  *
  * This clever technique was proposed by Miles Sabin:
  * http://chuusai.com/2011/06/09/scala-union-types-curry-howard
  *
  * Usage:
  *
  * {{{
  *   import com.gensler.scalavro.util.Union._
  *
  *   type UnionISB = union [Int] #or [String] #or [Boolean] #apply
  *
  *   def unionPrint[T : prove [UnionISB] #containsType](t: T) =
  *     t match {
  *       case i: Int     => println(i)
  *       case s: String  => println(s)
  *       case b: Boolean => println(b)
  *     }
  *
  *   unionPrint(true) // "true"
  *   unionPrint(55)   // 55
  * }}}
  */
object Union extends UnionTypeHelpers {

  type union[A] = {
    type or[B] = Disjunction [not[A]] #or [B] #apply
    type apply = not[not[A]]
  }

  type prove[U] = { type containsType[X] = not[not[X]] <:< U }

  /**
    * Constructs a new unary union instance with the supplied type as its only
    * member type.
    */
  def unary[T: TypeTag] = new Union[union [T] #apply]

  /**
    * == INTERNAL API ==
    *
    * Returns a new Union constructed from a conjunction of Union.not[_] types
    * and an additional type to add to the conjunction.
    *
    * Given `C = not[A] with not[B]` and `T`, this method returns a Union
    * wrapping the equivalent of `union[A] #or [B] #or [T]`.
    *
    * A `TypeTag` describing the conjunction of negations unwrapped from the
    * underlying union type is readily available via its
    * `underlyingConjunctionTag` method.
    *
    * Usage:
    * 
    * {{{
    * val binaryUnion = new Union[union [Int] #or [String]]
    *
    * val ternaryUnion = Union.combine(
    *   binaryUnion.underlyingConjunctionTag,
    *   typeTag[Boolean]
    * )
    *
    * ternaryUnion.typeMembers // yields Vector(Int, String, Boolean)
    *
    * }}}
    *
    * @tparam C the starting conjunction of negations
    * @tparam T the type to add to the conjunction before negation
    */
  protected[scalavro] def combine[C : TypeTag, T : TypeTag] = {
    new Union()(typeTag[Union.not[C with Union.not[T]]])
  }

}

private[scalavro] trait UnionTypeHelpers {

  sealed trait not[-A] {
    type or[B] = Disjunction[A] #or[B] #apply
    type apply = not[A]
    type unapply = A
  }

  trait Disjunction[A] {
    type or[B] = Disjunction[A with not[B]]
    type apply = not[A]
  }
}

class Union[U <: Union.not[_] : TypeTag] {

  import Union._

  type underlying = U

  type containsType[X] = prove [U] #containsType [X]

  val underlyingTag = typeTag[U]

  val underlyingConjunctionTag = {
    val ut = typeOf[U]
    val tParams = ut.typeSymbol.asType.typeParams // List[Symbol]
    ReflectionHelpers.tagForType(tParams.head.asType.toTypeIn(ut))
  }

  case class Value[T](ref: T, tag: TypeTag[T])

  protected var wrappedValue: Value[_] = Value((), typeTag[Unit])

  /**
    * Returns the set of member types of the underlying union.
    */
  def typeMembers(): Seq[Type] = {
    val ut = typeOf[U]
    val tParams = ut.typeSymbol.asType.typeParams // List[Symbol]
    val actualParam = tParams.head.asType.toTypeIn(ut)

    val notType = typeOf[Union.not[_]]
    var members = Vector[Type]()

    actualParam.foreach { part => if (part <:< notType) {
      val partParams = part.typeSymbol.asType.typeParams.map { _.asType.toTypeIn(part).normalize }
      members ++= partParams
    }}

    members.distinct.toIndexedSeq
  }

  /**
    * Returns `true` if the supplied type is a member of this union.
    */
  def contains[X: TypeTag]: Boolean = typeOf[not[not[X]]] <:< typeOf[U]

  def assign[X : TypeTag : containsType](newValue: X) {
    wrappedValue = Value(newValue, typeTag[X])
  }

  def rawValue() = wrappedValue.ref

  def value[X : TypeTag : containsType](): Option[X] = wrappedValue match {
    case Value(x, tag) if tag.tpe <:< typeOf[X] => Some(x.asInstanceOf[X])
    case _ => None
  }

  /**
    * == Java API ==
    */
  @throws[ClassCastException]
  def value[P](prototype: P) = wrappedValue.ref.asInstanceOf[P]

}
