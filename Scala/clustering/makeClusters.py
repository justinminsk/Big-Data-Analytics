#!/usr/bin/python

import random
import csv

c1 = [1,9]
c2 = [1,1]
c3 = [5,5]
c4 = [9,1]
c5 = [9,9]
centroids = [c1, c2, c3, c4, c5]

def make_cluster(centroid, n, s):
	cluster = []
	for i in range(n):
		x = random.normalvariate(centroid[0], s)
		y = random.normalvariate(centroid[1], s)
		label = "{}{}{}".format(centroid[0], centroid[1], i)
		cluster.append((label, x, y))
	return cluster

def make_clusters(centroids, n, s):
	clusters=[]
	for points in centroids:
		clusters += make_cluster(points, n, s)
	return clusters

points = make_clusters(centroids, 5, 0.5)

file = open("points.csv", "w")
writer = csv.writer(file)
writer.writerow(["label","x", "y"])

for point in points:
	writer.writerow(point)
file.close()