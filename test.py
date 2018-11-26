import numpy as np


def main():
	isImproved = False
	print("Improved") if isImproved else print("Not improved")

	reservoir = []
	adjacencyList = {}
	#var vertex2Triangles: Map[Int, Int] = Map()
	triangleCount = 0
	t = 0



	mVal = [214, 2140, 21400, 214000]
	for m in mVal:
		mean = 0
		for i in range(1000):
			f = open('dataset/randomData.txt')
			for line in f:
				# if(t % 10000 == 0):
				# 	print("t: ", t, " triCnt: ", triangleCount)
				t = t + 1
				vertices = line.split(" ")
				u = int(vertices[0])
				v = int(vertices[1])
				sampleResults = sampleEdge(t, m, reservoir, adjacencyList, triangleCount, isImproved)
				sample = sampleResults[0]
				reservoir = sampleResults[1]
				adjacencyList = sampleResults[2]
				triangleCount = sampleResults[3]

				if (isImproved):
					triangleCount = updateCounters('+', u, v, adjacencyList, triangleCount, t, m, isImproved)


				if(sample):
					#Add edge (u, v) to reservoir
					reservoir.append((u, v))
					#Add u/v as neighbour to v/u
					if u in adjacencyList.keys():
						adjacencyList[u].add(v)
					else :
						adjacencyList[u]= set([v])

					if v in adjacencyList.keys():
						adjacencyList[v].add(u)
					else :
						adjacencyList[v] = set([u])
					if(not isImproved):
						triangleCount = updateCounters('+', u, v, adjacencyList, triangleCount, t, m, isImproved)

			#println("iter: " + i + " final count: " + triangleCount)
			mean = mean + triangleCount
			triangleCount = 0
			t = 0
			reservoir = []
			adjacencyList = {}
			#vertex2Triangles = Map()

		print("M: ", m, " Average count: ", (mean/20))

def sampleEdge(t, m, res, adjacencyList, triCount, isImproved):

	if(t <= m):
		return True, res, adjacencyList, triCount
	else:
		sampleValue = np.random.rand()
		prob = m/t
		if (sampleValue <= prob):
			edgeIdx = np.random.randint(m - 1)
			removedEdge = res[edgeIdx]
			del res[edgeIdx]
			remU = removedEdge[0]
			remV = removedEdge[1]

			#remove from adjacencyList

			if remV in adjacencyList.keys():
				neighV = adjacencyList[remV]
				neighV.remove(remU)
			else:
				neighV = set(0)

			if(len(neighV) == 0):
				adjacencyList.pop(remV, None)
			else:
				adjacencyList[remV] = neighV

			if remU in adjacencyList.keys():
				neighU = adjacencyList[remU]
				neighU.remove(remV)
			else :
				neighU = set()


			if(len(neighU) == 0):
				adjacencyList.pop(remU, None)
			else:
				adjacencyList[remU] = neighU

			if (not isImproved):
				triCount = updateCounters('-', remU, remV, adjacencyList, triCount, t, m, isImproved)

			return True, res, adjacencyList, triCount

		return False, res, adjacencyList, triCount

def updateCounters(operator, u, v, adjList, triangleCount, t, m, isImproved):

	if u in adjList.keys():
		nSu = adjList[u]
	else :
		nSu = set()

	if v in adjList.keys():
		nSv = adjList[v]
	else :
		nSv = set()

	nSuv = nSu.intersection(nSv)
	updateVal = 0

	if (isImproved):
		firstTerm = (t - 1)/m
		secondTerm = (t - 2)/(m - 1)
		weightedUpdate = firstTerm * secondTerm
		updateVal = max(1, weightedUpdate)

	#println("op: " + operator + " triCount: " + triangleCount + " nSuv: " + nSuv)

	if (operator == '+'):
		for c in nSuv:
			if(isImproved):
				triangleCount += updateVal
			else:
				triangleCount += 1

	elif(operator == '-'):
		for c in nSuv:
			triangleCount -= 1

	#println("updated Tri cnt: " + triangleCount)
	return triangleCount

main()
