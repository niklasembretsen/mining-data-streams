import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object MiningDataStreams {

	def main(args: Array[String]) {

		// Create streaming context
	    val conf = new SparkConf().setAppName("Lab 3").setMaster("local[*]")
	    val ssc = new StreamingContext(conf, Seconds(1))
	    
	}
}