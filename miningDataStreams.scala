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

	    //read file from socket
		//try {
	    val lines = ssc.socketTextStream("localhost", 8888).cache()

		lines.foreachRDD( rdd => {
			rdd.collect().map( x => 
				{
					t = t + 1
					println(t)
				 	val u: Int = x.split(" ")(0).toInt
					val v: Int = x.split(" ")(1).toInt
					sampleEdge(u, v, t) match{
						case true => {
							adjacencyList += (u -> (adjacencyList.get(u).getOrElse(Set.empty) + v))
							adjacencyList += (v -> (adjacencyList.get(v).getOrElse(Set.empty) + u))
							reservoir :+= (u, v)
							updateCounters('+', u, v)
						}
						case _ =>
					}
				}
			)}
		)
		

			ssc.start()
		//} catch {
		//	case e: IllegalArgumentException => {
			ssc.awaitTermination()
		//	e.printStackTrace
		//		}
		//	}
	}

	def sampleEdge(u: Int, v: Int, t: Int): Boolean = {
		val m: Int = 60000

		if (t <= m) {
			//Inserted into the reservoir if t is less than M
			return true
		}
		else if(Random.nextInt(t) <= m) {
			//"coin flip" with p(H) = m/t and H => sample/replace
			//Delete an edge uniformly at random
			val edgeIdx: Int = Random.nextInt(reservoir.length)
			val edge: (Int, Int) = reservoir.remove(edgeIdx)
			val u: Int = edge._1
			val v: Int = edge._2
			val newUSet: Set[Int] = adjacencyList.get(u).get - v
			newUSet.size match {
				case 0 => adjacencyList -= u
				case _ => adjacencyList += (u -> newUSet)
			}

			val newVSet: Set[Int] = adjacencyList.get(v).get - u
			newVSet.size match {
				case 0 => adjacencyList -= v
				case _ => adjacencyList += (v -> newVSet)
			}

			updateCounters('-', u, v)
			return true
		}
		return false
	}

	def updateCounters(operator: Char, u: Int, v: Int) {
		val n_u = adjacencyList.get(u).getOrElse(Set.empty)
		val n_v = adjacencyList.get(v).getOrElse(Set.empty)
		val commonNeighbours: Set[Int] = n_u.intersect(n_v)
		operator match {
			case '+' => {
				//print(" + " + " commonNeighbours: " + commonNeighbours)
				triangleCount += commonNeighbours.size
				vertex2Triangles +=  (u -> (commonNeighbours.size + vertex2Triangles.get(u).getOrElse(0)))
				vertex2Triangles +=  (v -> (commonNeighbours.size + vertex2Triangles.get(v).getOrElse(0)))
				commonNeighbours.foreach( x =>  {
					vertex2Triangles += (x -> (vertex2Triangles.get(x).getOrElse(0) + 1 ))
				})
			}
			case _ => {
				//print(" - " + " commonNeighbours: " + commonNeighbours)
				triangleCount -= commonNeighbours.size
				vertex2Triangles +=  (u -> (vertex2Triangles.get(u).getOrElse(0) - commonNeighbours.size))
				vertex2Triangles +=  (v -> (vertex2Triangles.get(v).getOrElse(0) - commonNeighbours.size))
				commonNeighbours.foreach( x =>  {
					vertex2Triangles += (x -> (vertex2Triangles.get(x).getOrElse(0) - 1 ))
				})
			}
		}
		println(" c: " + triangleCount)
		println(" adjList: " + adjacencyList.size)
		println("ad " + adjacencyList)
		println(" reservoir: " + reservoir.size)
		println(reservoir)
	}

}