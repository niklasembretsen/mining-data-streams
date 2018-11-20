import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object MiningDataStreams {

	def main(args: Array[String]) {

		// Create streaming context
	    val conf = new SparkConf().setAppName("Lab 3").setMaster("local[*]")
	    val ssc = new StreamingContext(conf, Seconds(1))

	    //read file from socket
	    val lines = ssc.socketTextStream("localhost", 9999)

		lines.map(x => (x.split(" ")(0), x.split(" ")(1))).print()

		ssc.start()
		ssc.awaitTermination()
	}
}