"""Simulation of Server requests """
from __future__ import division
import urllib2
import csv
import argparse
import sys
import logging

class Queue(object):
    """ Python Queue object """
    def __init__(self):
        """ Queue Constructor """
        self.items = []

    def isEmpty(self):
        """ returns true if the array is empty """
        return self.items == []

    def enqueue(self, item):
        """ Adds an item to the front of the queue """
        self.items.insert(0, item)

    def dequeue(self):
        """ Removes an item from the end of the queue and returns the value """
        return self.items.pop()

    def size(self):
        """ Returns the length of the queue """
        return len(self.items)

class Server(object):
    """ A server object class """
    def __init__(self):
        """ Constructor - initializes currentTask and timeRemaining variables """
        self.currentTask = None
        self.timeRemaining = 0

    def tick(self):
        """ If a task is running, decrement counter by 1, if counter is 0, clear the task """
        if self.currentTask:
            self.timeRemaining = self.timeRemaining - 1
            if self.timeRemaining <= 0:
                self.currentTask = None

    def busy(self):
        """ If there is a task running, return True """
        return self.currentTask

    def startNext(self, newTask):
        """ A task can be a request object that has the getProcess method"""
        self.currentTask = newTask
        self.timeRemaining = newTask.getProcess()

class Request(object):
    """ A web request object """
    def __init__(self, time, processTime):
        """ Constructor, initializes the time, process time variables """
        self.timestamp = time
        self.process = processTime

    def getStamp(self):
        """ getter for timestamp """
        return self.timestamp

    def getProcess(self):
        """ getter for process """
        return self.process

    def waitTime(self, currentTime):
        """ how much time to wait """
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
    """ Simulate One Server Function """
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
    """ Simulate Many Servers function """
    serverList = [Server() for _ in range(servers)]
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
            serverList[activeServer].startNext(nextTask)
        serverList[activeServer].tick()
    averageWait = waitTime[-1] / len(waitTime)
    print("Many Server wait time {}").format("%10.7f" % averageWait)

def main():
    """ Main method, initialize argument parsers, set defaults """
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
