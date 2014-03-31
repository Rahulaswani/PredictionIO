package TwitterTrendDetection

import breeze.linalg._
import com.twitter.scalding._
import scala.List
import scala.util.parsing.combinator._
import scala.collection.mutable

/**
 * Created by vincent on 2/8/14.
 *
 * data_length : data length to test at the end of each time series
 * time : time at which we want to find trending items (eventually simply the last item of each series)
 * result_length : number of trending items to show (eventually, simply show all items with a score above 1).
 * influence : influence radius of each sample
 * samples : directory of the samples
 */
class TwitterTrendDetection(args: Args) extends Job(args) {

  object DoubleListParser extends RegexParsers {
    def apply(s: String) = parse("List(" ~> repsep("\\d+(\\.\\d*)".r ^^ (_.toDouble), ",")  <~ ")", s)
  }

  /////////////////ALGO PARAMS
  val data_length = args("data_length").toInt
  val t = args("time").toInt
  val result_length = args("result_length").toInt
  val y = args("influence").toDouble;

  ///////////////////////////////////////////////////////
  //---------- Read Input
  ///////////////////////////////////////////////////////

  val input = TextLine(args("input"))
  //Read the file
  val time_series=
    input.mapTo('id, 'observations) {
      line: String =>
        val raw = line.split("\t").toList

        val id = raw(0)
        val values:List[Double] = raw.drop(1).map(_.toDouble).slice(t -data_length, t)

        (id, values)
    }

  val trending_samples = TextLine(args("samples") + "/trending/part-00000")
    .mapTo('samples) {
    line : String =>
      DoubleListParser(line).get
  }

  val not_trending_samples = TextLine(args("samples") + "/not_trending/part-00000")
    .mapTo('samples) {
    line : String =>
      DoubleListParser(line).get
  }


  ///////////////////////////////////////////////////////
  //--------- Get trending items
  ///////////////////////////////////////////////////////

  val trending_votes = time_series.crossWithTiny(trending_samples) .mapTo(('id, 'samples, 'observations) -> ('id, 'trending_vote)){
    x : (String, List[Double], List[Double]) =>
      val id = x._1
      val samples = x._2
      val observations = x._3

      (id, Distance(samples, observations))
  }.groupBy('id){
    _.sum[Double]('trending_vote -> 'trending_votes)

  }

  val not_trending_votes = time_series.crossWithTiny(not_trending_samples) .mapTo(('id, 'samples, 'observations) -> ('id, 'not_trending_vote)){
    x : (String, List[Double], List[Double]) =>
      val id = x._1
      val samples = x._2
      val observations = x._3

      (id, Distance(samples, observations))
  }.groupBy('id){
    _.sum[Double]('not_trending_vote -> 'not_trending_votes)
  }

  //Compute trending factor and get the top n trending items
  val results = trending_votes.joinWithSmaller('id -> 'id, not_trending_votes).mapTo(('id, 'trending_votes, 'not_trending_votes) -> ('id, 'trending_factor)) {
    x : (String, Double, Double) =>
      val id = x._1
      val trending_vote = x._2
      val not_trending_vote = x._3
      //println(trending_vote)
      //println(not_trending_vote)
      (id, trending_vote / not_trending_vote)
  }.groupAll{
    _.sortedReverseTake[(Double, String)](('trending_factor, 'id) -> 'top, 250)
  }.flattenTo[(Double, String)]('top -> ('trending_factor, 'id))


  ///////////////////////////////////////////////////////
  //--------- Writing output
  ///////////////////////////////////////////////////////

  val output = Csv(p = args("output"), separator = "\t")
  results.joinWithLarger('id -> 'id, time_series)
    .groupAll{_.sortBy('trending_factor)}
    .write(output)


  ///////////////////////////////////////////////////////
  //--------- Distance between two series
  ///////////////////////////////////////////////////////

  //In general, the examples s will be much longer than the observations o. In that case, we look for the “best match” between s and o and define the distance d(s,o)
  // to be the minimum distance over all dim(s)-sized chunks of o.
  def Distance(s:List[Double], o:List[Double]) : Double = {
    var min_dist:Double = Int.MaxValue

    for (i <- 0 to s.length - o.length){
      val dist:Double = SumEuclideanDistance(s.slice(i, i + o.length), o)

      min_dist = math.min(dist, min_dist)
    }
    //println(min_dist)
    return min_dist
  }


  def SumEuclideanDistance(s:List[Double], o:List[Double]) : Double = {
    //Sum all the distances
    /*var sum : Double = 0.0
    val no = NormalizeList(o)
    val ns = NormalizeList(s)
    for (i<-0 to ns.length - 2){
      val speeds : Double = ns(i+1) - ns(i)
      val speedo : Double= no(i+1) - no(i)
      sum += EuclideanDistance(speeds,speedo)
     }*/
    var summ = 0.0
    for ( (as, ao) <- (s zip o)){
      summ += math.exp(-y *EuclideanDistance(as, ao))
    }
    return summ
  }

  def NormalizeList(l:List[Double]) : List[Double] = {
    val ratio = 100 / math.max(l.max, 10)
    return l.map(_  * ratio)
  }

  def EuclideanDistance(s:Double, o:Double) : Double = {
    //We expect both point to share the same x (time) value.
    //We can simplify the distance to an absolute difference.

    return math.abs(o - s)
  }
}
