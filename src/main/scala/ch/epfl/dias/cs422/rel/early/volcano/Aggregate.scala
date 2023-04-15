package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
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


  protected var aggregatedList = List.empty[(Tuple, Vector[Tuple])]

  override def open(): Unit = {
    input.open()
    var next = input.next()
    if (next == NilTuple && groupSet.isEmpty) {
      // return aggEmptyValue for each aggregate.
      aggregatedList = List(
        (IndexedSeq.empty[Elem] -> Vector(
          aggCalls
            .map(aggEmptyValue)
            .foldLeft(IndexedSeq.empty[Elem])((a, b) => a :+ b)
        ))
      )

    } else {
      var aggregates = Map.empty[Tuple, Vector[Tuple]]
      while (next != NilTuple) {
        val key: Tuple = groupSet.toArray.map(i => next.get(i))
        aggregates = aggregates.get(key) match {
          case Some(arr) => aggregates + (key -> (arr :+ next.get))
          case _ => aggregates + (key -> Vector(next.get))
        }
        next = input.next()
      }

      aggregatedList = aggregates.toList
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    aggregatedList match {
      case (key, tuples) :: tail =>
        aggregatedList = tail
        Some(
          key.++(
            aggCalls.map(agg =>
              tuples.map(t => agg.getArgument(t)).reduce(aggReduce(_, _, agg))
            )
          )
        )
      case _ => NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
