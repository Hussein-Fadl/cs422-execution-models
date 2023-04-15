package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, Tuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Stitch]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class Stitch protected(
                        left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
                        right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
                      ) extends skeleton.Stitch[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {

  private var leftLateTuple: Option[LateTuple] = NilLateTuple
  private var rightLateTuple: Option[LateTuple] = NilLateTuple
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    leftLateTuple = NilLateTuple
    rightLateTuple = NilLateTuple
    left.open()
    right.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    if (leftLateTuple.isEmpty) {
      leftLateTuple = left.next()
      if (leftLateTuple.isEmpty) {
        return NilLateTuple
      }
    }

    if (rightLateTuple.isEmpty) {
      rightLateTuple = right.next()
      if (rightLateTuple.isEmpty) {
        return NilLateTuple
      }
    }

    val lid = leftLateTuple.get.vid
    val rid = rightLateTuple.get.vid
    val stitched = leftLateTuple.get.value :++ rightLateTuple.get.value

    //In case the two tuples match i.e. have a common virtual ID, then we stitch the two tuples.
    if (lid == rid) {
      rightLateTuple = NilLateTuple
      Some(LateTuple(lid, stitched))
    } else {
      leftLateTuple = NilLateTuple
      next()
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
