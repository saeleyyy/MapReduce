# import MRJob
from mrjob.job import MRJob

# MapReduce Class
class evidenceCodeCount(MRJob):
    # mapper function
    def mapper(self, _, line):
        # split on tabs
        line_array = line.split("\t") 
        
        # get evidence code from line
        e_code = line_array[6]

        # only count records with e_code IPI
        if e_code == "IPI":
            # send to key/value pair to reducer
            yield e_code, 1
    
    # reducer function
    def reducer(self, key, values):
        # find sum of all unique id's
        yield key, sum(values)

# on start - run evdienceCodeCount class
if __name__ == '__main__':
   evidenceCodeCount.run()
