import mailparser
from pyspark import SparkContext
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
mailsBody = []

from os import walk
import os
filesn = []

#dossier contenant les utilisateurs
folder = os.listdir("/mnt/c/Users/thoma/Desktop/mailfoo/")
i = 1
is_noun = lambda pos: pos[:2] == 'NN'
for fold in folder :
    liste = []
   
    if(i<15):
        for root, dirs, files in os.walk("/mnt/c/Users/thoma/Desktop/mailfoo/"+fold+"/_sent_mail", topdown = False):
            for name in files:
                liste.append(os.path.join(root, name))
        
        for f in liste:
            try:
                str = open(f, 'r').read()
                mail = str.split("-----Original Message-----")[0]
                sentence = mailparser.parse_from_string(mail).body
                pos_tagged_sent = nltk.pos_tag(nltk.tokenize.word_tokenize(sentence))
                nouns = [tag[0] for tag in pos_tagged_sent if tag[1]=='NN']
                mailsBody.append(nouns)
            except ValueError:
                print("Oops!  That was no valid number.  Try again...")
    
        print('I have %d / %s' % (i, len(folder)))
    i = i + 1


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

print(final)

