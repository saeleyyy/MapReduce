# Task One: Identify the ten most commonly used GO terms for Escherichia coli only

# # import MRJob library
from mrjob.job import MRJob
from mrjob.step import MRStep

# MapReduce Class
class evidenceCodeCount(MRJob):
    def steps(self):
            return [
                MRStep(mapper=self.mapper,
                    reducer=self.reducer),
                MRStep(reducer = self.reducerTwo)
                ]
    # mapper function
    def mapper(self, _, line):
        # split on tabs
        line_array = line.split("\t")
        
        # get GO term  from line
        GO_term = line_array[4]

        # send to key/value pair to reducer
        yield GO_term, 1

    # reducer function
    def reducer(self, key, values):
        # find sum of all unique GO terms
        yield None, (key, sum(values))

    # second reducer function
    def reducerTwo(self, key, values):
        # append all vlaues to list
        list= []
        for index in values:
                list.append(index)
        
        # find the top 10
        count = len(list)
        for i in range(count-10,count):
            yield list[i]

# on start - run evdienceCodeCount class
if __name__ == '__main__':
   evidenceCodeCount.run()
