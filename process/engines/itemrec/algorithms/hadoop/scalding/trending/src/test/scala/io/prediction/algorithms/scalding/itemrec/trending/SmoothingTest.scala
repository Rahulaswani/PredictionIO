package io.prediction.algorithms.scalding.itemrec.trending

import org.specs2.mutable._

import com.twitter.scalding._

import io.prediction.commons.filepath.{ DataFile, AlgoFile }

class SmoothingTest extends Specification with TupleConversions {
	//implement rouding later

	def roundingData() = {

	}

	def testFunction(testArgs: Map[String, String], //specify window size here
		testInput: List[(String,List[Double])],
		testOutput: List[(String,List[Double])]) = {

    val appid = 1
    val engineid = 2
    val algoid = 3
    val hdfsRoot = "testroot/"

    JobTest("io.prediction.algorithms.scalding.itemrec.trending.Smoothing")
    .arg("appid", appid.toString)
    .arg("engineid", engineid.toString)
    .arg("algoid", algoid.toString)
    .arg("hdfsRoot", hdfsRoot)
    .arg("windowSize", testArgs("windowSize"))
    .source(Tsv(DataFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "ratings.tsv")),testInput)
    .sink[(String,List[Double])](Tsv(DataFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "ratings.tsv"))){ outputBuffer =>
    	"correctly calculate smothed timeseries" in {
    		outputBuffer.toList must containTheSameElementsAs(testOutput)
    	}
    }
    .run
    .finish
	}

	val test1args = Map[String,String]("windowSize" -> "2")

	val test1Input = List(
		("i1",List(1.0,2.0,3.0))
		)
	val test1Output = List(
		("i1",List(1.0,2.0,3.0))
		)

	"Smoothing window defined" should {
		testFunction(test1args, test1Input, test1Output)
	}

	}