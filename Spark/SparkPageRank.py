from pyspark import SparkContext
import re


def filterLinks(title, links):
    #outlinks = re.findall("\\[\\[(.*?)\\]\\]", links)
    #list(filter(lambda k: '|' in k, outlinks))
    #if title in outlinks:
    outlinks = re.findall("\\[\\[(.*?)\\]\\]", links)
    outlinksv2=[]
    for link in outlinks:
        if "|" in link:
            splitted = link.split("|")
            if (splitted[0] == title) or (splitted[0] in outlinksv2):
                continue

            outlinksv2.append(splitted[0])

    return outlinksv2




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
