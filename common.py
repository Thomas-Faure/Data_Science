import csv
from os import walk
import os

path = []
bannedWords = []

def setConfig(path,bannedWords):

    with open('./configurations.txt') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                path.append(row[0])
                line_count += 1
            else:
                for word in row:
                    bannedWords.append(word)

def getBannedWords():
    return bannedWords

def getPath():
    return path[0]

def getFolders(numberOfFolder):
    i = 1
    liste = []
    folder = os.listdir(getPath())
    for fold in folder :
        if(i<numberOfFolder):
            for root, dirs, files in os.walk(getPath()+"/"+fold+"/_sent_mail", topdown = False):
                for name in files:
                    liste.append(os.path.join(root, name))
    return liste

setConfig(path,bannedWords)

"""print(getBannedWords())
print(getPath())
print(getFolders(2))"""
