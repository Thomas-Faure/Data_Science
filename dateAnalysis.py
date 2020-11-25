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
mailsDay= []
mailsDayMonth = []
mailsNounMonth = []
mailsMonth = []
from os import walk
import os
from datetime import datetime
filesn = []
import plotly.express as px
import pandas as pd

bannedWord = ["cc","enron.com","http","re","draft","enronxg","subject","copi","@","aol.com",">"]

dictNumericalDay = {0:"Lundi", 1:"Mardi", 2:"Mercredi", 3:"Jeudi", 4:"Vendredi", 5:"Samedi", 6:"Dimanche"}
dictNumericalMonth = {"01":"Janvier", "02":"Février", "03":"Mars", "04":"Avril", "05":"Mai", "06":"Juin", "07":"Juillet", "08":"Aout", "09":"Septembre", "10":"Octobre", "11":"Novembre", "12":"Décembre"}

#dossier contenant les utilisateurs
folder = os.listdir("/mnt/c/Users/thoma/Desktop/maildir")
i = 1
is_noun = lambda pos: pos[:2] == 'NN'
for fold in folder :
    liste = []
   
    if(i<15):
        for root, dirs, files in os.walk("/mnt/c/Users/thoma/Desktop/maildir/"+fold+"/_sent_mail", topdown = False):
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
                mailsDayHour.append((dictNumericalDay[date.weekday()],hour))

                #GET MAIL BY DAY
                mailsDay.append(dictNumericalDay[date.weekday()])

                #GET MAIL DAY-MONTH
                month = date.strftime("%m")
                #mailsDayMonth.append((dictNumericalDay[date.weekday()],dictNumericalMonth[str(month)]))

                #GET NOUN-MONTH
                pst = PorterStemmer()
            
               
                mailsMonth.append(dictNumericalMonth[str(month)])
                nouns_lower = [x.lower() for x in nouns]
                difference = set(nouns_lower).symmetric_difference(set(bannedWord))
                cleanedNouns =  []
                for e in nouns_lower:
                    if not (e in bannedWord):
                        cleanedNouns.append(e)

                for noun in cleanedNouns:
                 
                    mailsNounMonth.append((pst.stem(noun),dictNumericalMonth[str(month)]))

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


#TO COMPUTE DAY-HOUR
rdd = sc.parallelize(mailsDay)
wordsPairRDDReduced = rdd.map(lambda a: (a,1))
DayReduced = wordsPairRDDReduced.reduceByKey(lambda a, b: a+b)

#TO COMPUTE DAY-MONTH
"""rdd = sc.parallelize(mailsDayMonth)
wordsPairRDDReduced = rdd.map(lambda a: (a,1))
DayMonthReduced = wordsPairRDDReduced.reduceByKey(lambda a, b: a+b)"""

#TO COMPUTE MONTH
rddMonth = sc.parallelize(mailsMonth)
wordsPairRDDReducedMonth = rddMonth.map(lambda a: (a,1))
MonthReduced = wordsPairRDDReducedMonth.reduceByKey(lambda a, b: a+b)


#TO COMPUTE NOUN-MONTH
rdd = sc.parallelize(mailsNounMonth)
wordsPairRDDReduced = rdd.map(lambda a: (a,1))
NounMonthReduced = wordsPairRDDReduced.reduceByKey(lambda a, b: a+b)
finalDate = DateReduced.map(lambda x: (datetime.strptime(x[0],"%d/%m/%Y"),x[1])).sortByKey(False)
finalHour = HourReduced.map(lambda x: (x[1], x[0])).sortByKey(False).take(20)
finalMonth =  MonthReduced.map(lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0]))
finalDay = DayReduced.map(lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0]))
#finalDayMonth = DayMonthReduced.map(lambda x: (x[1], x[0])).sortByKey(False).take(20)
finalNounMonth = NounMonthReduced.map(lambda x: (x[1], x[0])).sortByKey(False).take(20)

#final.saveAsTextFile("result")
print(finalDate)
df = pd.DataFrame(finalDate.collect(),columns=['date','occurence'])
figFinalDate = px.bar(df, y='occurence', x="date")
figFinalDate.update_xaxes(rangeslider_visible=True)
figFinalDate.show()


df = pd.DataFrame(finalDay.collect(),columns=['day','occurence'])
print(df)
figFinalDay = px.bar(df, y='occurence', x="day")
figFinalDay.show()

df = pd.DataFrame(finalMonth.collect(),columns=['month','occurence'])
print(df)
figFinalDay = px.bar(df, y='occurence', x="month")
figFinalDay.show()

print(finalHour)
#print(finalDayMonth)
print(finalNounMonth)


