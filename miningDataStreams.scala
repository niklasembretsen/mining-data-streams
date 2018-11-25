import org.apache.spark.SparkContext
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
	var triangleCount: Int = 0
	var t: Int = 0
	val m: Int = 171000
	val doImpr: Boolean = true

	def main(args: Array[String]) {

		val isStream: Boolean = false
		val isSpark: Boolean = false

		if (isStream){
			// Create streaming context
		    val conf = new SparkConf().setAppName("Lab 3").setMaster("local[*]")
		    val ssc = new StreamingContext(conf, Seconds(1))

		    val lines = ssc.socketTextStream("localhost", 9999).cache()

			lines.foreachRDD( rdd => {
				rdd.collect().map( x => 
					{
						t = t + 1
						println(t)
					 	val u: Int = x.split(" ")(0).toInt
						val v: Int = x.split(" ")(1).toInt
						sampleEdge(t) match{
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
			ssc.awaitTermination()
		}
		else if (isSpark){
			val conf = new SparkConf().setAppName("Lab 3").setMaster("local[*]")
		    val sc = new SparkContext(conf)

		    val data = sc.textFile("dataset/randomData.txt")
		    data.foreach(println)
		}
		else {
			val seeds: List[Int] = List(9, 17, 97, 931, 1011, 7313)
			val f = scala.io.Source.fromFile("dataset/randomData.txt").getLines.toSeq
			for(seed <- seeds) {
				for(edge <- f) {
					t = t + 1
					val u: Int = edge.split(" ")(0).toInt
					val v: Int = edge.split(" ")(1).toInt
					if (doImpr) {
						updateCounters('+', u, v)
					}
					sampleEdge(t, seed) match{
						case true => {
							adjacencyList += (u -> (adjacencyList.get(u).getOrElse(Set.empty) + v))
							adjacencyList += (v -> (adjacencyList.get(v).getOrElse(Set.empty) + u))
							reservoir :+= (u, v)
							if(!doImpr){
								updateCounters('+', u, v)
							}
						}
						case _ =>
					}
				}
				println("seed: " + seed)
				println("total triangles: " + triangleCount)
				reservoir = ListBuffer()
				adjacencyList = Map()
				vertex2Triangles = Map()
				triangleCount = 0;
				t = 0;
			}
		}
	}

	def sampleEdge(t: Int, seed: Int = 931): Boolean = {
		//val m: Int = 50000
		val r = new Random(seed)
		if (t <= m) {
			//Inserted into the reservoir if t is less than M
			return true
		}
		else if(r.nextInt(t) <= m) {
			//"coin flip" with p(H) = m/t and H => sample/replace
			//Delete an edge uniformly at random by sampling an index uniformly from the
			//reservoir
			val edgeIdx: Int = r.nextInt(reservoir.length)
			//Extract the two vertices and remove from the adjacency list
			//and adjust the triangle count
			val edge: (Int, Int) = reservoir.remove(edgeIdx)
			val u: Int = edge._1
			val v: Int = edge._2
			//Extract the neighbours of u and remove v
			val newUSet: Set[Int] = adjacencyList.get(u).get - v
			//if u has no more neighbours, remove u from the adjacency list
			//else add u's neighbour - v
			newUSet.size match {
				case 0 => adjacencyList -= u
				case _ => adjacencyList += (u -> newUSet)
			}
			//Extract the neighbours of v and remove u
			val newVSet: Set[Int] = adjacencyList.get(v).get - u
			//if u has no more neighbours, remove u from the adjacency list
			//else add u's neighbour - v
			newVSet.size match {
				case 0 => adjacencyList -= v
				case _ => adjacencyList += (v -> newVSet)
			}
			//adjust counters after removing u and v from the reservoir
			if(!doImpr) {
				updateCounters('-', u, v)
			}
			return true
		}
		return false
	}

	def updateCounters(operator: Char, u: Int, v: Int) {
		//get all neighbours of u
		val n_u = adjacencyList.get(u).getOrElse(Set.empty)
		//get all neighbours of u
		val n_v = adjacencyList.get(v).getOrElse(Set.empty)
		//get common neighbours of v and u => triangles
		val commonNeighbours: Set[Int] = n_u.intersect(n_v)
		//increase/decrease the count depending on if the edge was
		//added or removed

		if (doImpr) {
			val tempUpdate: Int = ((t - 1)*(t - 2))/(m*(m - 1))
			val updateVal: Int = Math.max(1, tempUpdate)
			for (c <- commonNeighbours) {
				triangleCount += updateVal
				vertex2Triangles +=  (u -> (updateVal + vertex2Triangles.get(u).getOrElse(0)))
				vertex2Triangles +=  (v -> (updateVal + vertex2Triangles.get(v).getOrElse(0)))
				vertex2Triangles += (c -> (vertex2Triangles.get(c).getOrElse(0) + updateVal))
			}
		}
		else {
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
					vertex2Triangles +=  (u -> (vertex2Triangles.get(u).getOrElse(0) - commonNeighbours.size))
					vertex2Triangles +=  (v -> (vertex2Triangles.get(v).getOrElse(0) - commonNeighbours.size))
					commonNeighbours.foreach( x =>  {
						vertex2Triangles += (x -> (vertex2Triangles.get(x).getOrElse(0) - 1 ))
					})
				}
			}
		}
		// if(t % 10000 == 0) {
		// 	println("t: " + t)
		// 	println(" c: " + triangleCount)
		// }
	}

}