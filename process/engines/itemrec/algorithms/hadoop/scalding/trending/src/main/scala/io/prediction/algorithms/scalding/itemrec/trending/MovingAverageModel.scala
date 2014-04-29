package io.prediction.algorithms.scalding.itemrec.trending

abstract class MovingAverageModel{
	def smoothing(timeseries: List[Double], window: Int): List[Double]
}