from pyspark import SparkContext
import re
import sys
import time

#find the all links in <text>...</text>, filter and format some particular links
def filterLinks(title, links):
    wiki_piped_links = re.findall("\\[\\[(.*?)\\]\\]", links)  # found all links
    wiki_links = []

    for link in wiki_piped_links:
        splitted = link

        if "[[" in link:
            lastIndexOfDoubleSquaredBrackets = splitted.rindex("[[", 0, len(splitted))
            splitted = splitted[0:lastIndexOfDoubleSquaredBrackets]

        if "|" in link:
            lastIndexOfPipe = splitted.rindex("|", 0, len(splitted))  # splitting links with pipes
            splitted = splitted[0:lastIndexOfPipe]  # keeping only the part before the pipe

        if (splitted == title) or (splitted in wiki_links):  # link already present or auto-referencing
            continue

        wiki_links.append(splitted.strip())  # list of parsed links

    return wiki_links #return the 'title' outlinks

#compute and retrieve the title and its adjacencyList (outlinks)
def parsePages(page):
    title = re.findall("<title>(.*)</title>", page) #retrieve title of pages
    text = re.findall("<text(.*?)</text>", page) #retrieve all the text between <text>...</text>
    outlinks = filterLinks(title[0].strip(), text[0])

    return title[0].strip(), outlinks

#function in flatMap transformation
def distributeRankToOutlinks(father, outlinks, rank):
    num_outlinks = len(outlinks)
    list = [(father, 0)] # to include the 'father' of the outlinks in the list
    for link in outlinks:
        list.append((link, rank / num_outlinks))
    return list

#compute the PageRank of a page
def computeNewRank(lastRank):
    return float((0.15 * ((1 / float(numberOfPages))) + 0.85 * lastRank))

address = "hdfs://namenode:9820/user/hadoop/"
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Number of parameters are " + str(len(sys.argv))+"! The number of parameters must be equal to three!")
        sys.exit(1)

    #initialize a new Spark Context
    sc = SparkContext(appName="WikiPageRank", master="yarn")

    #delete some log lines (this option can be removed)
    sc.setLogLevel("ERROR")

    # build an RDD from input file
    lines = sc.textFile(address + str(sys.argv[1]))

    # map transformation on each line
    # example of tuple-->('title', ['outlink1', 'outlink2', ....., 'outlinkN'])
    titles = lines.map(lambda page: parsePages(page)).cache()

    #Count number of pages
    numberOfPages = titles.count()

    #initialize rank for each page with 1/numberOfPages
    #example of (K,V) structure --> ('title', InitialPageRank)
    ranks = titles.map(lambda page: (page[0], float(1.0 / float(numberOfPages))))

    #cicle for compute the PageRank of pages
    for iteration in range(10):
        #JOIN TRANSFORMATION -->combine outlinks and pageRank of a page
        #example of structure of contributions AFTER join: [('title', ([listOfOutlinks], pageRank))....]
        #FLATMAP TRANSFORMATION --> compute the rank to be distributed among the outlinks
        #example of structure of contributions AFTER flatMap: [('title', incomingPageRank)....]
        contributions = titles.join(ranks).flatMap(lambda page: distributeRankToOutlinks(page[0], page[1][0], page[1][1]))

        #REDUCEBYKEY TRANSFORMATION --> sum all incomingPageRanks of a page --> (page, partialPageRank)
        #MAPVALUES TRANSFORMATION --> compute the PageRank of the page with the formula
        ranks = contributions.reduceByKey(lambda x, y: x + y).mapValues(lambda rank: computeNewRank(rank))

    #Order the pages by PageRank and save the total rank in a file
    #pageRanksOrdered = ranks.takeOrdered(ranks.count(), key=lambda x: -x[1])
    #pageRanksOrdered = sc.parallelize(pageRanksOrdered)
    #pageRanksOrdered.saveAsTextFile('SparkPageRank.txt')
    pageRankOrdered = ranks.sortBy(lambda a: -a[1])
    pageRankOrdered.saveAsTextFile(sys.argv[2])
    sc.stop()