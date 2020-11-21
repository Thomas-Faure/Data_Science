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
mailsDayHour = []
mailsDate = []
mailsDayMonth = []
mailsNounMonth = []
from os import walk
import os
from datetime import datetime
filesn = []
import plotly.express as px
import pandas as pd


#dossier contenant les utilisateurs
folder = os.listdir("/home/guillaume/Documents/POLYTECH/IG5/DataScienceAvancée/enron_mail_20150507/maildir")
i = 1
is_noun = lambda pos: pos[:2] == 'NN'
for fold in folder :
    liste = []
   
    if(i<6):
        for root, dirs, files in os.walk("/home/guillaume/Documents/POLYTECH/IG5/DataScienceAvancée/enron_mail_20150507/maildir/"+fold+"/_sent_mail", topdown = False):
            for name in files:
                liste.append(os.path.join(root, name))
        for f in liste:
            try:
                content = open(f, 'r').read()
                mail = content.split("-----Original Message-----")[0]
                date = mailparser.parse_from_string(mail).date
                sentence = mailparser.parse_from_string(mail).body
                pos_tagged_sent = nltk.pos_tag(nltk.tokenize.word_tokenize(sentence))
                nouns = [tag[0] for tag in pos_tagged_sent if tag[1]=='NN']

                #GET MAIL DATE
                date_time = date.strftime("%d/%m/%Y")
                mailsDate.append(date_time)

                #GET MAIL DAY-HOUR
                hour = date.strftime("%H")
                mailsDayHour.append(str(date.weekday())+'-'+hour)

                #GET MAIL DAY-MONTH
                month = date.strftime("%m")
                mailsDayMonth.append(str(date.weekday())+'-'+month)

                #GET NOUN-MONTH
                pst = PorterStemmer()
                for noun in nouns:
                    mailsNounMonth.append((pst.stem(noun),month))

            except ValueError:
                print("Oops!  That was no valid number.  Try again...")
    
        print('I have %d / %s' % (i, len(folder)))
    i = i + 1

#print(mailsDate)
#print(mailsNounMonth)



#TO COMPUTE DATE
rdd = sc.parallelize(mailsDate)
wordsPairRDDReduced = rdd.map(lambda a: (a,1))
DateReduced = wordsPairRDDReduced.reduceByKey(lambda a, b: a+b)


#TO COMPUTE DAY-HOUR
rdd = sc.parallelize(mailsDayHour)
wordsPairRDDReduced = rdd.map(lambda a: (a,1))
HourReduced = wordsPairRDDReduced.reduceByKey(lambda a, b: a+b)

#TO COMPUTE DAY-MONTH
rdd = sc.parallelize(mailsDayMonth)
wordsPairRDDReduced = rdd.map(lambda a: (a,1))
DayMonthReduced = wordsPairRDDReduced.reduceByKey(lambda a, b: a+b)


#TO COMPUTE NOUN-MONTH
rdd = sc.parallelize(mailsNounMonth)
wordsPairRDDReduced = rdd.map(lambda a: (a,1))
NounMonthReduced = wordsPairRDDReduced.reduceByKey(lambda a, b: a+b)
#ATTENTION IL FAUT FILTRER LES DONNEES ICI OU AVANT!!


finalDate = DateReduced.map(lambda x: (x[1], x[0])).sortByKey(False).take(20)
finalHour = HourReduced.map(lambda x: (x[1], x[0])).sortByKey(False).take(20)
finalDayMonth = DayMonthReduced.map(lambda x: (x[1], x[0])).sortByKey(False).take(20)
finalNounMonth = NounMonthReduced.map(lambda x: (x[1], x[0])).sortByKey(False).take(20)

#final.saveAsTextFile("result")
print(finalDate)
print(finalHour)
print(finalDayMonth)
print(finalNounMonth)


