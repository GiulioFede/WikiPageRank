from pyspark import SparkContext
import re
import pprint


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

    return wiki_links


def parsePages(page):
    title = re.findall("<title>(.*)</title>", page)
    text = re.findall("<text(.*?)</text>", page)
    outlinks = filterLinks(title[0].strip(), text[0])

    return title[0].strip(), outlinks


def distributeRankToOutlinks(father, outlinks, rank):
    num_outlinks = len(outlinks)
    list = [(father, 0)]
    for link in outlinks:
        list.append((link, rank / num_outlinks))
    return list


def computeNewRank(lastRank):
    if lastRank == 0:
        return float(1.0 / numberOfPages)
    else:
        return float((0.15 * (float(1 / numberOfPages)) + 0.85 * lastRank))


if __name__ == "__main__":

    sc = SparkContext(appName="WikiPageRank", master="yarn")
    sc.setLogLevel("ERROR")

    lines = sc.textFile("hdfs://namenode:9820/user/hadoop/input/wiki-micro.txt")

    titles = lines.map(lambda page: parsePages(
        page)).cache()  # esempio di tupla --> ('Image:Lynne Slater2.jpg', ['EastEnders', 'Lynne Hobbs', 'Category:EastEnders images'])

    numberOfPages = titles.count()

    ranks = titles.map(lambda page: (page[0], float(1.0 / numberOfPages)))

    for iteration in range(10):
        contributions = titles.join(ranks).flatMap(
            lambda page: distributeRankToOutlinks(page[0], page[1][0], page[1][1]))

        ranks = contributions.reduceByKey(lambda x, y: x + y).mapValues(lambda rank: computeNewRank(rank))

    # titles = []
    # for result in ranks.collect():
    #     if(result[0] in titles):
    #         print("Doppione",result[0])
    #     titles.append(result[0])

    pageRanksOrdered = ranks.takeOrdered(ranks.count(), key=lambda x: -x[1])
    pageRanksOrdered = sc.parallelize(pageRanksOrdered)
    pageRanksOrdered.saveAsTextFile('sparkOutput_giulio_prova1.txt')

    sc.stop()