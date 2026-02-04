package effpi.process

import scala.reflect.{ClassTag, TypeTest}

// TypeTest instance for matching against union/sum types using ClassTag
given classTaggedTypeTest[A, B](using ct: ClassTag[B]): TypeTest[A, B] with {
  def unapply(x: A): Option[x.type & B] = {
    // Handle primitive type boxing - map primitive classes to their boxed equivalents
    val runtimeClass = ct.runtimeClass match {
      case java.lang.Integer.TYPE => classOf[java.lang.Integer]
      case java.lang.Long.TYPE => classOf[java.lang.Long]
      case java.lang.Double.TYPE => classOf[java.lang.Double]
      case java.lang.Float.TYPE => classOf[java.lang.Float]
      case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
      case java.lang.Byte.TYPE => classOf[java.lang.Byte]
      case java.lang.Short.TYPE => classOf[java.lang.Short]
      case java.lang.Character.TYPE => classOf[java.lang.Character]
      case other => other
    }

    if (runtimeClass.isInstance(x)) Some(x.asInstanceOf[x.type & B])
    else None
  }
}

given emptyWrap[A]: WrapMatches[A, EmptyTuple] with {
  type Wrapped = EmptyTuple
  def wrap(matches: EmptyTuple): EmptyTuple = EmptyTuple
}

given consWrap[A, B, F, Tail <: Tuple, TailWrapped <: Tuple](using
  ev: F <:< (B => Process),
  tt: TypeTest[A, B],
  tailWrapper: WrapMatches.Aux[A, Tail, TailWrapped]
): WrapMatches[A, F *: Tail] with {
  type Wrapped = MatchCase[A, B, Process] *: TailWrapped
  def wrap(matches: F *: Tail): MatchCase[A, B, Process] *: TailWrapped = {
    val head = matches.head
    val tail = matches.tail
    MatchCase(ev(head).asInstanceOf[B => Process], tt) *: tailWrapper.wrap(tail)
  }
}
