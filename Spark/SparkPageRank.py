from pyspark import SparkContext
import re
import pprint


def filterLinks(title, links):
    outlinks = re.findall("\\[\\[(.*?)\\]\\]", links)
    outlinksv2=[]
    for link in outlinks:
        splitted = link

        if "|" in link:
            lastIndexOfPipe = splitted.rindex("|", 0,len(splitted))
            splitted = splitted[0:lastIndexOfPipe]

        if (splitted == title) or (splitted in outlinksv2):
            continue

        outlinksv2.append(splitted)

    return outlinksv2


def parsePages(page):
    title = re.findall("<title>(.*)</title>", page)
    text = re.findall("<text(.*?)</text>", page)
    outlinks = filterLinks(title, text[0])

    return title[0], outlinks


sc = SparkContext(appName="WikiPageRank", master="yarn")
sc.setLogLevel("ERROR")

wiki_micro = sc.textFile("hdfs://namenode:9820/user/hadoop/input/wiki-micro.txt")

lines = wiki_micro.count()
mapPhase = wiki_micro.map(lambda e: parsePages(e))
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(mapPhase.collect())

sc.stop()
