package io.prediction.algorithms.scalding.itemrec.trending
import com.twitter.scalding._
import io.prediction.commons.filepath.{ DataFile, AlgoFile }
class Trending(args: Args) extends Job(args) {
	  // args
  val hdfsRootArg = args("hdfsRoot")

  val appidArg = args("appid").toInt
  val engineidArg = args("engineid").toInt
  val algoidArg = args("algoid").toInt
  val evalidArg = args.optional("evalid") map (x => x.toInt)

  val windowSize = args("windowSize").toInt

  val timeseriesRaw = Tsv(DataFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "ratings.tsv")).read
  val smoothedtimeseries = Tsv(AlgoFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "itemRecScores.tsv"))
  val timeseries = timeseriesRaw.mapTo((0, 1) -> ('iid, 'timeseries)){
  	fields: (String, String) => 
  	  val (iid, timeseriesstring) = fields
  	  (iid, timeseriesstring.split(",").toList.map(_toDouble))
  }

  timeseries.mapTo('timeseries -> 'processedtimeseries){
  	timeseries: List[Double] => 
  	val smoothingmodel = new DoNothing();
  	val smoothed = smoothingmodel.smoothing(timeseries,windowSize)
  	smoothed
  }.write(smoothedtimeseries)

}