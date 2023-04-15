package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class LateJoin(
               left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               condition: RexNode
             ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  private val leftKeys = getLeftKeys
  private var it = Iterator.empty[LateTuple]
  private var mapRight = Map.empty[Tuple, IndexedSeq[LateTuple]]
  private var index = 0L

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()
    index = 0L

    val rightKeys = getRightKeys
    var loadedRight = IndexedSeq.empty[LateTuple]
    var next = right.next()
    while(next != NilLateTuple) {
      loadedRight = loadedRight :+ next.get
      next = right.next()
    }
    mapRight = loadedRight.groupBy(t => rightKeys.map(t.value(_)))
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    if (it.hasNext) {
      val entry = it.next()
      index = index + 1
      Some(entry)
    } else {
      left.next() match {
        case Some(t) =>
          it = iterate(t)
          next()
        case _ => NilLateTuple
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    it = Iterator.empty[LateTuple]
  }

  private def iterate(entry: LateTuple): Iterator[LateTuple] = {
    mapRight.get(leftKeys.map(entry.value(_))) match {
      case Some(entries) =>
        (for (e <- entries)
          yield LateTuple(
            index,
            entry.value :++ e.value
          )).iterator
      case _ => Iterator.empty
    }
  }
}
