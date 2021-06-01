import re

from pyspark import SparkContext
import re

def parsePages(page):
    title = re.findall("<title>(.*)</title>", page)
    text = re.findall("<text>(.*?)</text>")
    outlinks = page.findall("\[\[(.*?)\]\]")

    return title[0], outlinks

sc = SparkContext(appName="WikiPageRank", master="yarn")
sc.setLogLevel("ERROR")

wiki_micro = sc.textFile("hdfs://namenode:9820/user/hadoop/input/wiki-micro.txt")

lines = wiki_micro.count()
print(lines)

sc.stop()
