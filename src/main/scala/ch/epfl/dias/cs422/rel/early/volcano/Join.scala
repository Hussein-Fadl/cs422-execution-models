package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private val leftKeys = getLeftKeys
  private val rightKeys = getRightKeys
  private var it = Iterator.empty[Tuple]
  private var mapRight = Map.empty[Tuple, IndexedSeq[Tuple]]
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    mapRight = right.toIndexedSeq.groupBy(t => rightKeys.map(t))
  }
  /**
    * @inheritdoc
    */

  override def next(): Option[Tuple] = {
    if (it.hasNext) {
      Some(it.next())
    } else {
      left.next() match {
        case NilTuple => NilTuple
        case Some(t) =>
          it = mapRight.get(leftKeys.map(t)) match {
            case Some(tuples) =>
              (for (e <- tuples) yield t :++ e).iterator
            case _ => Iterator.empty
          }
          next()
      }
    }
  }


  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
