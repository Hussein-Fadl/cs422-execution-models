package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected var sortedIterator: Iterator[Tuple] = Iterator()
  private var count = 0
  private var sortedList = Vector[Tuple]()


  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    count = fetch.getOrElse(-1)
    var next = input.next()
    while (next != NilTuple) {
      sortedList = sortedList :+ next.get
      next = input.next()
    }

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

    sortedList = sortedList.sorted(ordering)
    sortedIterator = sortedList.iterator.drop(offset.getOrElse(0))
  }
  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (count != 0 && sortedIterator.hasNext) {
      if (count > 0) {
        count = count - 1
      }
      Some(sortedIterator.next())
    } else {
      NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
    sortedList = Vector.empty[Tuple]
  }
}
