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
mailsBody = []
import plotly.express as px
from os import walk
import os
filesn = []
bannedWord = ["cc","enron.com","http","re","draft","enronxg","subject","copi","@","aol.com"]
#dossier contenant les utilisateurs
folder = os.listdir("/home/guillaume/Documents/POLYTECH/IG5/DataScienceAvancée/enron_mail_20150507/maildir")
i = 1
is_noun = lambda pos: pos[:2] == 'NN'
for fold in folder :
    liste = []
   
    if(i<2):
        for root, dirs, files in os.walk("/home/guillaume/Documents/POLYTECH/IG5/DataScienceAvancée/enron_mail_20150507/maildir/"+fold+"/_sent_mail", topdown = False):
            for name in files:
                liste.append(os.path.join(root, name))
        
        for f in liste:
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
#final.saveAsTextFile("result")
df = pd.DataFrame(final,columns=['occurence','nom'])
fig = px.bar(df, x='nom', y='occurence')
fig.write_image("./fig1.png")
fig.show()