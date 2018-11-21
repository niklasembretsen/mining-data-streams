import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType}
import scala.util.Random
import scala.collection.mutable.ListBuffer


object MiningDataStreams {

	// Reservoir
	var reservoir: ListBuffer[(Int, Int)] = ListBuffer()
	var adjacencyList: Map[Int, Set[Int]] = Map()
	var vertex2Triangles: Map[Int, Int] = Map()
	var triangleCount: Int = 0;
	var t: Int = 0;

	def main(args: Array[String]) {		

		// Create streaming context
	    val conf = new SparkConf().setAppName("Lab 3").setMaster("local[*]")
	    val ssc = new StreamingContext(conf, Seconds(1))

		// adjacencyList
		// var adjacencyList = Map[Int, Set[Int]] = Map()
		// var edge2Triangles = Map[Int, Int] = Map()
		// var triangleCount: Int = 0;
		// var t: Int = 0;

	    //read file from socket
	    val lines = ssc.socketTextStream("localhost", 9999)
		
		lines.map(x => {
			t = t + 1
			val u: Int = x.split(" ")(0).toInt
			val v: Int = x.split(" ")(1).toInt
			sampleEdge(u, v, t) match{
				case true => {
				// 	var currentUSet = adjacencyList.get(u).getOrElse(-1)
				// 	currentUSet match {
				// 		case -1 => {
				// 			adjacencyList += + (u -> Set(v))
				// 		}
				// 		case _ => {
				// 			adjacencyList += (u -> currentUSet + v)
				// 			updateCounters('+', u , v)
				// 		}
				// 	} 
				// 	var currentVSet = adjacencyList.get(v).getOrElse(-1)
				// 	currentVSet match{
				// 		case -1 => {
				// 			adjacencyList += (v -> Set(u))
				// 		}
				// 		case _ => {
				// 			adjacencyList += (v -> currentVSet + u)
				// 			updateCounters ('+', v, u)
				// 		}
				// 	}
					adjacencyList += (u -> (a.get(u).getOrElse(Set.empty) + v))
					adjacencyList += (v -> (a.get(v).getOrElse(Set.empty) + u))
					reservoir :+= (u, v)
					updateCounters('+', u, v)
				}
				case _ =>
			}
		})


		ssc.start()
		ssc.awaitTermination()
	}

	def sampleEdge(u: Int, v: Int,t: Int): Boolean = {
		val m: Int = 1000

		if (t <= m) {
			//Inserted into the reservoir if t is less than M
			return true
		}
		else if(Random.nextInt(t) >= m) {
			//"coin flip" with p(H) = m/t and H => sample/replace
			//Delete an edge uniformly at random
			val edgeIdx: Int = Random.nextInt(reservoir.length)
			val edge: (Int, Int) = reservoir.remove(edgeIdx)
			val u: Int = edge._1
			val v: Int = edge._2
			val newUSet: Set[Int] = adjacencyList.get(u).get - u
			newUSet match {
				case Set() => adjacencyList -= u
				case _ => adjacencyList += (u -> newUSet)
			}

			val newVSet: Set[Int] = adjacencyList.get(v).get - v
			newVSet match {
				case Set() => adjacencyList -= v
				case _ => adjacencyList += (v -> newVSet)
			}

			updateCounters('-', u, v)
			return true		
		}
		return false
	}

	def updateCounters(operator: Char, u: Int, v: Int) {
		val commonNeighbours: Set[Int] = adjacencyList.get(u).get.intersection(adjacencyList.get(v).get)
		operator match {
			case '+' => {
				triangleCount += commonNeighbours.size
				vertex2Triangles +=  (u -> (commonNeighbours.size + vertex2Triangles.get(u).getOrElse(0)))
				vertex2Triangles +=  (v -> (commonNeighbours.size + vertex2Triangles.get(v).getOrElse(0)))
				commonNeighbours.foreach( x =>  {
					vertex2Triangles += (x -> (vertex2Triangles.get(x).getOrElse(0) + 1 ))
				})
			}
			case _ => {
				triangleCount -= commonNeighbours.size
				vertex2Triangles +=  (u -> (commonNeighbours.size - vertex2Triangles.get(u).getOrElse(0)))
				vertex2Triangles +=  (v -> (commonNeighbours.size - vertex2Triangles.get(v).getOrElse(0)))
				commonNeighbours.foreach( x =>  {
					vertex2Triangles += (x -> (vertex2Triangles.get(x).getOrElse(0) - 1 ))
				})
			}
		}
	}

}