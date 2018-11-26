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

	def main(args: Array[String]) {
		val isImproved: Boolean = true
		val f = scala.io.Source.fromFile("dataset/randomData.txt").getLines.toSeq

		var reservoir: ListBuffer[(Int, Int)] = ListBuffer()
		var adjacencyList: Map[Int, Set[Int]] = Map()
		var vertex2Triangles: Map[Int, Int] = Map()
		var triangleCount: Int = 0
		var t: Int = 0
		//
		val mVal: Seq[Int] = Seq(214, 2140, 214000)
		for (m <- mVal) {
			//val m: Int = 10000
			var mean: Int = 0
			for (i <- 1 to 10) {
				for(edge <- f) {
					// if(t % 10000 == 0){
					// 	println("t: " + t + " triCnt: " + triangleCount)
					// }
					t = t + 1
					val u: Int = edge.split(" ")(0).toInt
					val v: Int = edge.split(" ")(1).toInt
					val sampleResults = sampleEdge(t, m, reservoir, adjacencyList, triangleCount, isImproved)
					val sample: Boolean = sampleResults._1
					reservoir = sampleResults._2
					adjacencyList = sampleResults._3
					triangleCount = sampleResults._4

					if (isImproved) {
						triangleCount = updateCounters('+', u, v, adjacencyList, triangleCount, t, m, isImproved)
					}

					if(sample) {
						//Add edge (u, v) to reservoir
						reservoir :+= (u, v)
						//Add u/v as neighbour to v/u
						adjacencyList = adjacencyList + (u -> (adjacencyList.get(u).getOrElse(Set.empty) + v))
						adjacencyList = adjacencyList + (v -> (adjacencyList.get(v).getOrElse(Set.empty) + u))
						if(!isImproved) {
							triangleCount = updateCounters('+', u, v, adjacencyList, triangleCount, t, m, isImproved)
						}
					}
				}
				//println("iter: " + i + " final count: " + triangleCount)
				mean = mean + triangleCount
				triangleCount = 0
				t = 0
				reservoir = ListBuffer()
				adjacencyList = Map()
				vertex2Triangles = Map()
			}
			println("M: " + m + " Average count: " + (mean/10))
		}
	}

	def sampleEdge(t: Int,
					m: Int,
					res: ListBuffer[(Int, Int)],
					adjList: Map[Int, Set[Int]],
					triCnt: Int,
					isImproved: Boolean): (Boolean, ListBuffer[(Int, Int)], Map[Int, Set[Int]], Int) = {

		if(t <= m) {
			return (true, res, adjList, triCnt)
		}
		else {
			val sampleValue: Double = Random.nextDouble()
			val prob: Double = m/t.toDouble
			//println("sampleVal: " + sampleValue + " prob: " + prob)
			if (sampleValue <= prob) {
				//println("yaay")
				//coinflip with p(H) = m/t
				val edgeIdx: Int = Random.nextInt(m)
				val removedEdge: (Int, Int) = res.remove(edgeIdx)
				val remU: Int = removedEdge._1
				val remV: Int = removedEdge._2
				var adjacencyList: Map[Int, Set[Int]] = adjList
				var triCount: Int = triCnt

				//remove from adjacencyList
				var neighV: Set[Int] = adjacencyList.get(remV).get
				neighV = neighV - remU

				if(neighV.size == 0) {
					adjacencyList = adjacencyList - remV
				}
				else {
					adjacencyList = adjacencyList + (remV -> neighV)
				}

				var neighU: Set[Int] = adjacencyList.get(remU).get
				neighU = neighU - remV

				if(neighU.size == 0) {
					adjacencyList = adjacencyList - remU
				}
				else {
					adjacencyList = adjacencyList + (remU -> neighU)
				}
				if (!isImproved){
					triCount = updateCounters('-', remU, remV, adjacencyList, triCnt, t, m, isImproved)
				}
				return (true, res, adjacencyList, triCount)
			}
			return (false, res, adjList, triCnt)
		}
	}

	def updateCounters(operator: Char,
						u: Int,
						v: Int,
						adjList: Map[Int, Set[Int]],
						triCount: Int,
						t: Int = 0,
						m: Int = 0,
						isImproved: Boolean): Int = {

		val nSu: Set[Int] = adjList.get(u).getOrElse(Set.empty)
		val nSv: Set[Int] = adjList.get(v).getOrElse(Set.empty)
		val nSuv: Set[Int] = nSu.intersect(nSv)
		var triangleCount: Int = triCount
		var updateVal: Int = 0

		if (isImproved) {
			val firstTerm: Int = (t - 1)/m
			val secondTerm: Int = (t - 2)/(m - 1)
			val weightedUpdate: Int = firstTerm * secondTerm
			updateVal = Math.max(1, weightedUpdate)
		}
		//println("op: " + operator + " triCount: " + triangleCount + " nSuv: " + nSuv)

		if (operator == '+') {
			for (c <- nSuv) {
				if(isImproved) {
					triangleCount += updateVal
				}
				else {
					triangleCount += 1
				}
			}
		}
		else if(operator == '-') {
			for (c <- nSuv) {
				triangleCount -= 1
			}
		}
		//println("updated Tri cnt: " + triangleCount)
		return triangleCount
	}

}