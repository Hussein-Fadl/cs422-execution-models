package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {
    val ordering: Ordering[Tuple] =
      if (collation.getFieldCollations.size() > 0) {
        collation.getFieldCollations
          .toArray(
            Array.ofDim[RelFieldCollation](collation.getFieldCollations.size)
          )
          .map(c => {
            val order = Ordering.by[Tuple, Comparable[Elem]](
              _(c.getFieldIndex).asInstanceOf[Comparable[Elem]]
            )
            if (c.direction.isDescending) {
              order.reverse
            } else {
              order
            }
          })
          .reduce(_.orElse(_))
      } else {
        Ordering.fromLessThan((_, _) => false)
      }

    val start = offset.getOrElse(0)
    val end = start + fetch.getOrElse(Int.MaxValue)
    input.execute()
      .transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.toIndexedSeq)
      .sorted(ordering)
      .slice(start, end)
      .transpose
  }
}
