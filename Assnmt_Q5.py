import nltk
import re
from nltk.stem import PorterStemmer
from nltk.stem.snowball import SnowballStemmer


input = sc.textFile("/mnt/%s/Bigram5.txt"%MOUNT_NAME)
stopWords = "a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your"
stemmer = PorterStemmer()
sentence = input.flatMap(lambda x:x.split("."))\
           .map(lambda x:x.strip(" ").split())\
           .filter(lambda x:len(x)!=0)
stem = sentence.map(lambda x:[re.sub('[^A-Za-z0-9]+','',x[i]).lower() for i in range(0,len(x))])\
       .map(lambda x:[stemmer.stem(x[i]) for i in range(0,len(x))])\
       .map(lambda x:[str(x[i]) for i in range(0,len(x))])\
       .map(lambda x:['be' if(x[i] in stopWords) else x[i] for i in range(0,len(x))])
biGram = stem.flatMap(lambda x:[((x[i],x[i+1]),1) for i in range(0,len(x)-1)])


output = biGram.reduceByKey(lambda x,y:x+y)\
         .filter(lambda x:x[1]>1)\
         .map(lambda x:('('+','.join(x[0])+')',str(x[1])))
    
output = output.collect()
for i in range(0,len(output)):
  print " ".join(reslt[i])
     