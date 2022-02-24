from mrjob.job import MRJob

#klase die overerft van MRJob
class MRWordCount(MRJob):
       # functie voor de map-fase in mapreduce
        # _ wil zeggen dat dit argument niet gebruikt wordt/ niet belangrijk is
        def mapper(self, _, line):
            for word in line.split():
                # yield is een beetje als return, maar gaat verder doen -> Alles wordt opgeslagen in een lijst
                yield (word, 1)
        # functie voor de reduce-fase
        def reducer(self, word, counts):
            yield (word, sum(counts))
        
# zeggen dat deze file de main file is
if __name__ == "__main__":
    MRWordCount.run()
