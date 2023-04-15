package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.qo.rules.{RemoveEmptyReconstructLeft, RemoveEmptyReconstructRight}
import org.apache.calcite.rel.rules.CoreRules

package object qo {
  val selectedOptimizations =
    List(
      CoreRules.PROJECT_REMOVE,
      RemoveEmptyReconstructLeft.INSTANCE,
      RemoveEmptyReconstructRight.INSTANCE
    )
}
