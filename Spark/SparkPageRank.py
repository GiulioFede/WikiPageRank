from pyspark import SparkContext
import re


def filterLinks(title, links, page):
    outlinks = re.findall("\\[\\[(.*?)\\]\\]", links)
    list(filter(lambda k: '|' in k, outlinks))

    if title in outlinks



def parsePages(page):
    title = re.findall("<title>(.*)</title>", page)
    text = re.findall("<text>(.*?)</text>", page)
    outlinks = filterLinks(title, text)

    return title[0], outlinks


sc = SparkContext(appName="WikiPageRank", master="yarn")
sc.setLogLevel("ERROR")

wiki_micro = sc.textFile("hdfs://namenode:9820/user/hadoop/input/wiki-micro.txt")

lines = wiki_micro.count()
print(lines)

sc.stop()
