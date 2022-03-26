import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes

class Mapper(api.Mapper):
    # key-values zitten in de context
    def map(self, context):
        # context.value is de lijn die we lezen
        for w in context.value.split():
            context.emit(w,1)
    
class Reducer(api.Reducer):
    # key-values zitten in de context    
    def reduce(self, context):
        context.emit(context.key, sum(context.values))
        
FACTORY = pipes.Factory(Mapper, reducer_class=Reducer)

def main():
    pipes.run_task(FACTORY)

## zeggen dat deze file de main file is
if __name__ == "__main__":
    main()
