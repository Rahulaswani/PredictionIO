import breeze.linalg._
import com.twitter.scalding._
import scala.collection.mutable
/**
 * Created by vincent on 2/16/14.
 */

/**
 * input : relative path to the training data.
 * threshold : value under which we consider data to be irrelevant (same as 0).
 * h_window : length of data for the first average (for edge detection).
 * w_window : length of data for the second average (for edge detection)
 * sample_length : length of one trending sample
 * result_length : number of trending and not trending samples
 * @param args
 */
class GenerateLearningData(args: Args) extends Job(args) {

  /////////////////INPUT FORMAT PARAMS
  val input = TextLine(args("input"))


  /////////////////EDGE DETECTION PARAMS
  /**
   * Under that number, will say that it is irrelevant.
   */
  val threshold = args("threshold").toDouble

  //Window 1 size
  val h =  args("h_window").toInt
  //Window 2 size
  val w =  args("w_window").toInt

  /////////////////GENERATED SAMPLES FORMAT PARAMS
  val sample_length = args("sample_length").toInt;
  val result_length = args("result_length").toInt;

  //------- Compute edges factor curves
  val trending_curves =
    input.mapTo('id, 'trending_factor_curves, 'values) {
      line: String =>
        val values = line.split("\t")

        val id = values(0)
        val targets:DenseVector[Double] = DenseVector[Double](values.drop(1).map(_.toDouble))

        val edge_detector = new EdgeDetector(h, w, threshold, targets);


        val trending_factor_curves:mutable.MutableList[Double] = mutable.MutableList[Double]()
        val value_curves:mutable.MutableList[Double] = mutable.MutableList[Double]()
        for (i <- 0 to targets.length){
          //We can compute edge outside this intervale
          if (i >= h && i <= (targets.length - w)){
            trending_factor_curves += edge_detector.getEdgeFactor(i)
          }
          //We cannot compute edge
          else {
            trending_factor_curves += 0
          }
        }
        (id, trending_factor_curves.toArray, targets.toArray)
    }


  //------- Search to all trending factor curves. Search the 500 highest peak.
  val samples =
    trending_curves.flatMapTo(('trending_factor_curves, 'values) -> ('factor, 'samples)){
      fields : ( Array[Double], Array[Double]) =>
        val trending_factor_curve:Array[Double] = fields._1;
        val values:List[Double] = fields._2.toList;

        val result:mutable.MutableList[(Double, List[Double])] = mutable.MutableList[(Double, List[Double])]()

        //Samples ending before h or after w are irrelevant (no edge factor available)
        val start = if (h > sample_length) h else sample_length
        for (i <- start to values.length - w){

          //Contains a time series portion. These
          val samples:List[Double] = values.slice(i - sample_length, i)

          //Trick to avoid having a list of 0 as a sample list
          if (samples.sum < threshold) {
            result += ((0, samples))
          }
          else {
            result += ((trending_factor_curve(i), samples))
          }


          //println("length " + trending_factor_curve(i) + " samples : " + values)
        }
        result.toList
    }

    //samples.write(Csv(args("output")+ "/samples"))


  val trending_samples = samples.groupAll
    {
      _.sortWithTake(('factor, 'samples) -> 'top, result_length)
      {
        (elem1:(Double, List[Double]), elem2:(Double, List[Double])) => elem1._1 > elem2._1
      }

    }.flattenTo[(Double, List[Double])]('top -> ('factor, 'samples)) //flatenTo as oppose to just flatten to exclude the intermediate top tuple.
    .project('samples)

  val not_trending_samples = samples.groupAll
    {
      _.sortWithTake(('factor, 'samples) -> 'top, result_length)
      {
        (elem1:(Double, List[Double]), elem2:(Double, List[Double])) => elem1._1 < elem2._1
      }

    }.flattenTo[(Double, List[Double])]('top -> ('factor, 'samples)) //flatenTo as oppose to just flatten to exclude the intermediate top tuple.
    .project('samples)

  trending_samples.write(TextLine(args("output") + "/trending"))
  not_trending_samples.write(TextLine(args("output") + "/not_trending"))
}
