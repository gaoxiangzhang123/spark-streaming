import java.util

import org.apache.spark.graphx.VertexId
import org.apache.spark.util.AccumulatorV2

class SetAccumulator extends AccumulatorV2[VertexId,java.util.Set[VertexId]]{

  private val _setArray: java.util.Set[VertexId] = new java.util.HashSet[VertexId]()

  override def isZero: Boolean = {
    _setArray.isEmpty
  }

  override def copy(): AccumulatorV2[VertexId, util.Set[VertexId]] = {
    val newAcc = new SetAccumulator()
    _setArray.synchronized{
      newAcc._setArray.addAll(_setArray)
    }
    newAcc
  }

  override def reset(): Unit = {
    _setArray.clear()
  }

  override def add(v: VertexId): Unit = {
    _setArray.add(v)
  }

  override def merge(other: AccumulatorV2[VertexId, util.Set[VertexId]]): Unit = {
    other match {
      case o: SetAccumulator => _setArray.addAll(o.value)
    }

  }

  override def value: util.Set[VertexId] = {
    java.util.Collections.unmodifiableSet(_setArray)
  }

}
