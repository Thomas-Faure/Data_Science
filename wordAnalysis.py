import mailparser
from pyspark import SparkContext
import pandas as pd
import string
import re
sc =SparkContext()
import os, nltk
from nltk.stem import LancasterStemmer
from nltk.stem import PorterStemmer
from nltk.corpus import wordnet as wn
from nltk import word_tokenize, pos_tag
nltk.download('wordnet')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
import plotly.express as px
from os import walk
import os
import common



#Configuration de l'environnement
bannedWord = common.getBannedWords()
mailsBody = []

#Parcours de emails et repartition du contenu
for f in common.getFolders(7):
    try:
        str = open(f, 'r').read()
        mail = str.split("-----Original Message-----")[0]
        sentence = mailparser.parse_from_string(mail).body
        pos_tagged_sent = nltk.pos_tag(nltk.tokenize.word_tokenize(sentence))
        nouns = [tag[0] for tag in pos_tagged_sent if tag[1]=='NN']

        nouns_lower = [x.lower() for x in nouns]
        difference = set(nouns_lower).symmetric_difference(set(bannedWord))
        cleanedNouns =  []
        for e in nouns_lower:
            if not (e in bannedWord):
                cleanedNouns.append(e)
        mailsBody.append(cleanedNouns)

    except ValueError:
        print("Oops!  That was no valid number.  Try again...")



rdd = sc.parallelize(mailsBody)

rdd2 = rdd.flatMap(lambda list: list)

wordRDD = rdd2.flatMap(lambda line: line.split(' '))

wordRDD2 = rdd2.filter(lambda x: x !="<" and x !="*" and x !=">" and x != "%")
array = wordRDD2.collect()
pst = PorterStemmer()

reducedWord = []
for word in array :
    reducedWord.append(pst.stem(word))

rddReduced = sc.parallelize(reducedWord)
wordRDDReduced = rddReduced.flatMap(lambda line: line.split(' '))
wordsPairRDDReduced = wordRDDReduced.map(lambda a: (a,1))
wordCountReduced = wordsPairRDDReduced.reduceByKey(lambda a, b: a+b)

final = wordCountReduced.map(lambda x: (x[1], x[0])).sortByKey(False).take(20)
df = pd.DataFrame(final,columns=['occurence','nom'])
fig = px.bar(df, x='nom', y='occurence')
fig.write_image("./graphiques/bar_words.png")
fig.show()