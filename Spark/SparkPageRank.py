from pyspark import SparkContext
import re
import pprint


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

        wiki_links.append(splitted)  # list of parsed links

    return wiki_links


def parsePages(page):
    title = re.findall("<title>(.*)</title>", page)
    text = re.findall("<text(.*?)</text>", page)
    outlinks = filterLinks(title, text[0])

    return title[0], outlinks


sc = SparkContext(appName="WikiPageRank", master="yarn")
sc.setLogLevel("ERROR")

wiki_micro = sc.textFile("hdfs://namenode:9820/user/hadoop/input/wiki-micro.txt")

lines = wiki_micro.count()
mapPages = wiki_micro.map(lambda e: parsePages(e))  # reading and parsing input file

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(mapPhase.collect())

mapInitialRank =

sc.stop()
