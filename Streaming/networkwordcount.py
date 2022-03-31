from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime

###### maak shared variabelen aan
def getWordExcludeList(sparkContext):
    if "worldExcludeList" not in globals():
        globals()["worldExcludeList"] = sparkContext.broadcast(["Hello"])
    return globals()["worldExcludeList"]

def getDroppedWordCounters(sparkContext):
    if "droppedWordCounters" not in globals():
        globals()["droppedWordCounters"] = sparkContext.accumulator(0)
    return globals()["droppedWordCounters"]

# streaming context aanmaken (gelijkaardig aan de sparksession)
sc = SparkContext("local[2]", "network_word_count")
sc.setLogLevel("ERROR") # only show errors (anders te veel overhead bij elke iteratie warnings)
ssc = StreamingContext(sc, 5) # elke 5 seconden
ssc.checkpoint("checkpoint")

# data inlezen van poort 9999
lines = ssc.socketTextStream("localhost", 9999)
# lines gaat alle lijnen data bevatten die toekomen binnen 5 seconden intervallen
# flatmap -> alle lijnen omzetten naar 1 lijn
words = lines.flatMap(lambda line: line.split(" "))
# emit tuples (word, 1)
pairs = words.map(lambda word: (word, 1))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x+y)

def skipWords(time, rdd):
    # haal de shared variabelen op
    excludeList = getWordExcludeList(rdd.context)
    droppedWordsCounter = getDroppedWordCounters(rdd.context)
    
    def func(row):
        # the whole row looks something like this (word, 1) -> it's literally a tuple so we get the first index which is zero, and hence the word itself.
        word = row[0]
        if word in excludeList.value:
            # the second index in the tuple is the counter, which is why we are then adding it to droppedWordsCounter
            print("word[1]: ", row[1])
            droppedWordsCounter.add(row[1])
            
            return False
        else:
            return True
    
    # this rdd.filter will be called on every word/row
    # the reason why every word is in a word is because we are applying the skipWords function on the wordCounts -> this is where we already split each word into a separate line.
    f = rdd.filter(func)
    
    print("# Genegeerde woorden:", droppedWordsCounter.value)
    print("Gefilterede rdd:", f.collect())
    
    pass

wordCounts.foreachRDD(skipWords)

# start applicatie
ssc.start()
ssc.awaitTermination() # zorg dat het blijft draaien
