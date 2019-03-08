from __future__ import division
import random
import urllib2
import csv
import re
import argparse
import sys
import logging
from datetime import datetime

class Queue(object):
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

class Server:
    def __init__(self):
        self.currentTask = None
        self.timeRemaining = 0

    def tick(self):
        if self.currentTask:
            self.timeRemaining = self.timeRemaining - 1
            if self.timeRemaining <= 0:
                self.currentTask = None

    def busy(self):
        if self.currentTask != None:
            return True
        else:
            return False

    def startNext(self, newTask):
        self.currentTask = newTask
        self.timeRemaining = newTask.getProcess()

class Request:
    def __init__(self, time, processTime):
        self.timestamp = time
        self.process = processTime

    def getStamp(self):
        return self.timestamp

    def getProcess(self):
        return self.process

    def waitTime(self, currentTime):
        return currentTime - self.timestamp

def downloadData(url):
    """Download the CSV at the url provided, return a URLlib response object"""
    try:
        req = urllib2.Request(url)
        response = urllib2.urlopen(req)
    except urllib2.HTTPError as error:
        LOGGER.error(error)
        sys.exit()
    except urllib2.URLError:
        LOGGER.error('Unable to retrieve CSV file')
        sys.exit()
    return response

def processData(csvdata):
    """Build out a list of dictionaries from the csv object """
    fieldnames = ("requestTime", "fileName", "processTime")
    datafile = csv.DictReader(csvdata, fieldnames=fieldnames)
    dictList = []
    for line in datafile:
        dictList.append(line)
    return dictList

def simulateOneServer(dictData):
    server = Server()
    queue = Queue()
    waitTime = []
    for row in dictData:
        reqSec = int(row['requestTime'])
        procTime = int(row['processTime'])
        request = Request(reqSec, procTime)
        queue.enqueue(request)

        if not server.busy() and not queue.isEmpty():
            nextTask = queue.dequeue()
            waitTime.append(nextTask.waitTime(reqSec))
            server.startNext(nextTask)

        server.tick()
    averageWait = waitTime[-1] / len(waitTime)
    print("Single Server wait time: {}").format("%10.7f" % averageWait)


def simulateManyServers(dictData, servers):
    serverList = [ Server() for i in range(servers)]
    queue = Queue()
    waitTime = []
    activeServer = 0
    for row in dictData:
        reqSec = int(row['requestTime'])
        procTime = int(row['processTime'])
        request = Request(reqSec, procTime)
        queue.enqueue(request)
        if activeServer == len(serverList) - 1:
            activeServer = 0
        else:
            activeServer += 1

        if not queue.isEmpty() and not serverList[activeServer].busy():
            nextTask = queue.dequeue()
            waitTime.append(nextTask.waitTime(reqSec))
            serverList[i].startNext(nextTask)
        serverList[activeServer].tick()
    averageWait = waitTime[-1] / len(waitTime)
    print("Many Server wait time {}").format("%10.7f" % averageWait)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--servers', help='Number of webservers')
    parser.add_argument('--url', help='URL of file to load')
    args = parser.parse_args()
    if args.url:
        url = args.url
    else:
        url = 'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'
    request = downloadData(url)
    workingDict = processData(request)
    if args.servers:
        cluster = args.servers
    else:
        cluster = 3
    simulateOneServer(workingDict)
    simulateManyServers(workingDict, cluster)

if __name__ == '__main__':
    LOGGER = logging.getLogger('assignment3')
    LOGGER.setLevel(logging.ERROR)
    try:
        LOGFILE = logging.FileHandler('errors.log')
    except IOError:
        print "Unable to open log file"
    LOGFILE.setLevel(logging.DEBUG)
    FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    LOGFILE.setFormatter(FORMATTER)
    LOGGER.addHandler(LOGFILE)
    main()