package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val filtered = input
      .execute()
      .transpose
      .filter(_.last.asInstanceOf[Boolean])

    if (filtered.isEmpty && groupSet.isEmpty) {
      IndexedSeq(
        aggCalls
          .map(aggEmptyValue)
          .foldLeft(IndexedSeq.empty[Elem])((a, b) => a :+ b)
          .asInstanceOf[Tuple] :+ true
      ).transpose
        .map(toHomogeneousColumn)
    } else {
      val keyIndices = groupSet.toArray.toIndexedSeq
      
      filtered
        .map(_.toIndexedSeq)
        .groupBy(tuple => keyIndices.map(i => tuple(i)))
        .map {
          case (key, tuples) =>
            (key ++
              aggCalls.map(agg =>
                tuples.map(t => agg.getArgument(t)).reduce(aggReduce(_, _, agg))
              )
              ) :+ true
        }
        .toIndexedSeq
        .transpose
        .map(toHomogeneousColumn)
    }
  }
}
