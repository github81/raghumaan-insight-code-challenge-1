#!/usr/bin/python

import sys, getopt, re, datetime

maxDT = ''
minDT = ''
tweetCount = 0
vertexDegree = 0
hashTags = {}
hashTagsCreatedAt = {}
hashTagsCreatedAtFlags = {}
outputFileOjb = ''

#function to the substring between two substrings in a string
def getBetweenStrings(s, first, last ):
	try:
		start = s.index( first ) + len( first )
		end = s.index( last, start)
		return s[start:end]	
	except ValueError:
		return ""

#function to get the max date/time and also calculate min date/time (-60 seconds)
def createMaxMinDateTime(s ):
	global maxDT
	global minDT

	tempDT = datetime.datetime.strptime(s, "%c")
	if maxDT:
		if tempDT > maxDT:
			maxDT = tempDT
			minDT = maxDT - datetime.timedelta(seconds=60)
	else:
		maxDT = tempDT
		minDT = maxDT - datetime.timedelta(seconds=60)

#function to get the hash tags in each tweet and store for future use	
def addHashTags(s, createdAt ):
	global hashTags
	global hashTagsCreatedAt
	global hashTagsCreatedAtFlags
	global tweetCount
	global maxDT
	global minDT

	hashTagStrings = s.split("},{")
	hashTagList = []
	nHashTag = ''
	for hashTagString in hashTagStrings:
		nHashTag = getBetweenStrings(hashTagString, '"text":"', '","indices"')
		#convert it to lower case
		nHashTag = nHashTag.lower()
		hashTagList.append(nHashTag)
	
	#store the tags, createdAt date/time 
	hashTags[tweetCount] = hashTagList
	createdAtDT = datetime.datetime.strptime(createdAt, "%c")
	hashTagsCreatedAt[tweetCount] = createdAtDT
	#if the created date/time is between min and max date/time then flag it
	if minDT <= createdAtDT <= maxDT:
		hashTagsCreatedAtFlags[tweetCount] = 1
	else:
		hashTagsCreatedAtFlags[tweetCount] = 0
	tweetCount += 1

#function to remove old tweets which are out max date/time and min date/time
def removeHashTags():
	global hashTagsCreatedAtFlags
	global hashTagsCreatedAt
	global hashTags
	global tweetCount

	for tweetId in range(0, tweetCount):
		#get the created date/time from the stored tweet
		#unflag it if is not between min and max date/time
		createdAtDT = hashTagsCreatedAt[tweetId]
		if maxDT < createdAtDT < minDT:
			hashTagsCreatedAtFlags[tweetId] = 0

#function to write average degree to an output file
def writeDegreeToFile():
	global vertexDegree
	global outputFileObj

	outputFileObj.write("%.2f\n" % vertexDegree)

#function to calculate the average degree on each new tweet		
def calculateDegree():
	global hashTagsCreatedAtFlags
	global hashTagsCreatedAt
	global hashTags
	global tweetCount
	global vertexDegree

	nodeConnectionFlags = {}
	hashTagsCount = {}

	for tweetId in range(0, tweetCount):
		#do not include the old tweets
		if hashTagsCreatedAtFlags[tweetId] is 0:
			continue
		#get the list of hash tags in a particular tweet
		#do not include in the calculation if there are 0 or 1 tweets
		hashTagList = hashTags[tweetId]
		if len(hashTagList) is 0 or len(hashTagList) is 1:
			continue

		#create the connection and also count the number of connections per node
		for hashTagL in hashTagList:
			for hashTagR in hashTagList:
				#no self connections
				if hashTagL == hashTagR:
					continue
				nodeConnection = hashTagL + '<->' + hashTagR
				nodeConnectionReverse = hashTagR + '<->' + hashTagL
				if nodeConnection in nodeConnectionFlags or nodeConnectionReverse in nodeConnectionFlags:
					continue
				nodeConnectionFlags[nodeConnection] = 1
				nodeConnectionFlags[nodeConnectionReverse] = 1

				#count the connections
				if hashTagL in hashTagsCount:
					hashTagsCount[hashTagL] += 1
				else:
					hashTagsCount[hashTagL] = 1
				if hashTagR in hashTagsCount:
					hashTagsCount[hashTagR] += 1
				else:
					hashTagsCount[hashTagR] = 1

		#calculate the degree here
		nodeCount = 0
		connectionCount = 0
		for hashTag in hashTagsCount:
			connectionCount += hashTagsCount[hashTag]
			nodeCount += 1

		#calculate here
		vertexDegree = float(connectionCount)/nodeCount

#main function
def main(argv):

	global outputFileObj

	# get the command line arguments
	inputfile = ''
	outputfile = ''
	try:
		opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
	except getopt.GetoptError:
		print 'AverageDegree.py -i <inputfile> -o <outputfile>'
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-h':
			print 'AverageDegree.py -i <inputfile> -o <outputfile>'
			sys.exit()
		elif opt in ("-i","--ifile"):
			inputfile = arg
		elif opt in ("-o","--ofile"):
			outputfile = arg

	# read the input file	
	tweetin = open(inputfile,'r')

	#also open a while to write the output
	outputFileObj = open(outputfile, 'w')

	for tweet in iter(tweetin):
		
		tweet = tweet.strip()

		#if the line is empty then ignore
		if not tweet:
			continue

		#if the line starts with {"limit": then ignore
		if tweet.startswith('{"limit":'):
			continue
		
		#get the craeted date/time	
		createdAt = getBetweenStrings(tweet, 'created_at":"', '","id":')
		#remove +0000
		createdAt = createdAt.replace('+0000 ','')
		hashTags = getBetweenStrings(tweet, '"hashtags":[','],"urls":')

		#get max date/time objects
		createMaxMinDateTime(createdAt)

		#read the hashtags
		addHashTags(hashTags, createdAt)

		#remove the old hash tags which are not created between min and max date/time
		removeHashTags()

		#calculate the vertex degree
		calculateDegree()

		#write the vertex degree to the output file
		writeDegreeToFile()		

	tweetin.close()
	outputFileObj.close()


if __name__=="__main__":
	main(sys.argv[1:])

