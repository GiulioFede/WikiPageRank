from pyspark import SparkContext
import re
import pprint
from operator import add


def filterLinks(title, links):
    wiki_piped_links = re.findall("\\[\\[(.*?)\\]\\]", links)  # found all links
    wiki_links = []

    for link in wiki_piped_links:
        splitted = link

        if "|" in link:
            lastIndexOfPipe = splitted.rindex("|", 0, len(splitted))  # splitting links with pipes
            splitted = splitted[0:lastIndexOfPipe]  # keeping only the part before the pipe

        if (splitted == title) or (splitted in wiki_links):  # link already present or auto-referencing
            continue

        wiki_links.append(splitted.strip())  # list of parsed links

    return wiki_links


def parsePages(page):
    title = re.findall("<title>(.*)</title>", page)
    text = re.findall("<text(.*?)</text>", page)
    outlinks = filterLinks(title[0].strip(), text[0])

    return title[0].strip(), outlinks, float(1/lines)


def computeContributions(pages, pageRank):
    count = len(pages)
    for page in pages:
        yield page, pageRank / count


sc = SparkContext(appName="WikiPageRank", master="yarn")
sc.setLogLevel("ERROR")

wiki_micro = sc.textFile("hdfs://namenode:9820/user/hadoop/input/wiki-micro.txt")

lines = wiki_micro.count()
mapPages = wiki_micro.map(lambda e: parsePages(e))  # reading and parsing input file

pageRanks = []

for iteration in range(10):
    contributions = mapPages.flatMap(lambda x: computeContributions(x[1], x[2]))
    pageRanks = contributions.reduceByKey(add).mapValues(lambda rank: 0.15*(1/float(lines)) + 0.85*rank)

pageRanksOrdered = pageRanks.takeOrdered(10, key=lambda x: -x[1])

i = 0
rankPrecedente = 0
for(link, rank) in pageRanksOrdered:
    if rankPrecedente != rank:
        i += 1

    print("%i: Title: %s Rank: %s" % (i, link, rank))
    rankPrecedente = rank

sc.stop()
