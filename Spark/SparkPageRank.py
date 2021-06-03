from pyspark import SparkContext
import re
import pprint
from operator import add


# filter valid links using regex
def filterLinks(title, links):
    wiki_piped_links = re.findall("\\[\\[(.*?)\\]\\]", links)  # found all links
    wiki_links = []

    for every_link in wiki_piped_links:
        splitted = every_link

        if "|" in every_link:  # if we have a pipe we must parse the link:
            lastIndexOfPipe = splitted.rindex("|", 0, len(splitted))  # 1) splitting links with pipes
            splitted = splitted[0:lastIndexOfPipe]  # 2) keeping only the part before the pipe

        if (splitted == title) or (splitted in wiki_links):  # link already present or auto-referencing
            continue

        wiki_links.append(splitted)  # list of parsed links

    return wiki_links


# we receive an xml file, we must parse it
def parsePages(page, lines):
    title = re.findall("<title>(.*)</title>", page)
    text = re.findall("<text(.*?)</text>", page)
    outlinks = filterLinks(title[0], text[0])

    return title[0], outlinks, float(1 / lines)  # return: title | <outlinks> | 1/N


# compute the rank to send
def computeContributions(pages, pageRank):
    count = len(pages)
    for page in pages:
        yield page, pageRank / count


# ****** PAGE RANK SPARK ****** #
sc = SparkContext(appName="WikiPageRank", master="yarn")
sc.setLogLevel("ERROR")

wiki_micro = sc.textFile("hdfs://namenode:9820/user/hadoop/input/wiki-micro.txt")

number_of_lines = wiki_micro.count()
mapPages = wiki_micro.map(lambda e: parsePages(e, number_of_lines))  # reading and parsing input file

pageRanks = []

for iteration in range(10):
    contributions = mapPages.flatMap(lambda x: computeContributions(x[1], x[2]))
    pageRanks = contributions.reduceByKey(add).mapValues(lambda r: 0.15 * float(1 / number_of_lines) + 0.85 * r)

pageRanksOrdered = pageRanks.takeOrdered(10, key=lambda x: -x[1])  # take the first 10 links ordered
fileToSave = sc.parallelize(pageRanksOrdered)
fileToSave.saveAsTextFile('sparkOutput_2.txt')

i = 0
rankPrecedente = 0

for (link, rank) in pageRanksOrdered:
    if rankPrecedente != rank:
        i += 1

    print("%i: Title: %s Rank: %s" % (i, link, rank))
    rankPrecedente = rank

sc.stop()
