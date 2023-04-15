package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val rightKeys = getRightKeys
    val leftKeys = getLeftKeys

    val loadedRight = right
      .execute()
      .transpose
      .filter(_.last.asInstanceOf[Boolean])
    val loadedLeft = left
      .execute()
      .transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.dropRight(1))

    if (loadedLeft.size > loadedRight.size) {
      val mapRight = loadedRight
        .groupBy(t => rightKeys.map(t(_)))

      loadedLeft
        .flatMap(t => {
          mapRight.get(leftKeys.map(t(_))) match {
            case Some(tuples) => tuples.map(t :++ _)
            case _ => IndexedSeq.empty
          }
        })
        .transpose
        .map(toHomogeneousColumn)
    } else {
      val mapLeft = loadedLeft
        .groupBy(t => leftKeys.map(t(_)))

      loadedRight
        .flatMap(t => {
          mapLeft.get(rightKeys.map(t(_))) match {
            case Some(tuples) => tuples.map(_ :++ t)
            case _ => IndexedSeq.empty
          }
        })
        .transpose
        .map(toHomogeneousColumn)
    }
  }
}
