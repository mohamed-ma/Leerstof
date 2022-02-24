# MET tee
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes
from itertools import tee
class Mapper(api.Mapper):
    def __init__(self, context):
        super(Mapper, self).__init__(context)
        context.set_status("initializing mapper")
        self.input_words = context.get_counter("WORDCOUNT", "INPUT_WORDS")
    
    def map(self, context):
        for w in context.value.split():
            if len(w) > 0 and w[0].isalpha():
                letter = w[0].lower()
                context.emit(letter, 1)
                context.increment_counter(self.input_words, 1)
            # er kunnen meerdere zaken ge-emit worden
            # besteed aandacht aan key -> de key mag niet 1 letter zijn, anders kan er conflicten zijn.
            # de conflict bestaat omdat er dan misschien dezelfde letter kan voorkomen in de key.
            # om dit tegen te gaan -> unieke key die niet zal voorkomen in een letter, woord, ...
            context.emit("lengte woord", len(w))
            
class Reducer(api.Reducer):
    def __init__(self, context):
        super(Reducer, self).__init__(context)
        context.set_status("initializing reducer")
        self.unique_letters = context.get_counter("WORDCOUNT", "DIFF_FIRTS_LETTERS")
    
    def reduce(self, context):
        if context.key == "lengte woord":
            som = 0
            it0, it1, it2 = tee(context.values, 3)
            aantal = 0
            for val in context.values:
                aantal += 1
                som += val
            context.emit("gemiddelde", som/aantal)
            
            # een andere manier met tee, om over de context.values te gaan itereren -> is een iterable en geen lijst.
            aantal = len(list(it1))
            som = sum(it2)
            print("mean", som/aantal)
            context.emit("mean", som/aantal)
        else:
            context.emit(context.key, sum(context.values))
            context.increment_counter(self.unique_letters, 1)
        
FACTORY = pipes.Factory(Mapper, reducer_class=Reducer)

def main():
    pipes.run_task(FACTORY)

if __name__ == "__main__":
    main()
    
