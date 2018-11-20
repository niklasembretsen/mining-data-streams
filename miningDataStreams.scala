import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType}


object MiningDataStreams {

	def main(args: Array[String]) {		

		// Create streaming context
	    val conf = new SparkConf().setAppName("Lab 3").setMaster("local[*]")
	    val ssc = new StreamingContext(conf, Seconds(1))

		// Reservoir	
		var reservoir = Map[Int, Set[Int]] = Map()
		var edge2Triangles = Map[Int, Int] = Map()
		var triangleCount: Int = 0;
		var t: Int = 0;

	    //read file from socket
	    val lines = ssc.socketTextStream("localhost", 9999)
		
		lines.map(x => {
			t = t + 1
			u = x.split(" ")(0)
			v = x.split(" ")(1)
			sampleEdge(u, v, t) match{
				case true => {
				// 	var currentUSet = reservoir.get(u).getOrElse(-1)
				// 	currentUSet match {
				// 		case -1 => {
				// 			reservoir += + (u -> Set(v))
				// 		}
				// 		case _ => {
				// 			reservoir += (u -> currentUSet + v)
				// 			updateCounters('+', u , v)
				// 		}
				// 	} 
				// 	var currentVSet = reservoir.get(v).getOrElse(-1)
				// 	currentVSet match{
				// 		case -1 => {
				// 			reservoir += (v -> Set(u))
				// 		}
				// 		case _ => {
				// 			reservoir += (v -> currentVSet + u)
				// 			updateCounters ('+', v, u)
				// 		}
				// 	}
					reservoir += (u -> (a.get(u).getOrElse(Set.empty) + v))
					reservoir += (v -> (a.get(v).getOrElse(Set.empty) + u))
					updateCounters('+', u, v)
				}
				case _ =>
			}
		})


		ssc.start()
		ssc.awaitTermination()
	}

	def sampleEdge(u: Int,
					v: Int,
					t: Int,
					reservoir: Map[Int, Set[Int]]): Boolean = {

		val m: Int = 1000
	}

	def updateCounters(operator: Char,
						u: Int,
						v: Int,
						reservoir: Map[Int, Set[Int]],
						edge2Triangles: Map[Int, Int],
						triangleCount: Int) {

	}

}