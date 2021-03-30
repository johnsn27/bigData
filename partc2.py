from mrjob.job import MRJob

class PartCJob2(MRJob):

    def mapper(self, _, line):
        try:
            job1_fields=line.split('\t')
            if(len(job1_fields) == 2):
                miners = job1_fields[0]
                size = int(job1_fields[1])
                miners = miners.replace('"','')
                yield (None, (miners, size))
        except :
            pass

    def combiner(self, _, unsorted_values):
        sorted_values = sorted(unsorted_values, reverse=True, key = lambda unsorted_vals:unsorted_vals[1])
        i = 0
        for value in sorted_values:
            yield ("Top", value)
            i += 1
            if i >= 10:
                break

    def reducer(self, _, unsorted_values):
        sorted_values = sorted(unsorted_values, reverse=True, key = lambda unsorted_vals:unsorted_vals[1])
        i = 0
        for values in sorted_values:
            yield("{} - {}".format(values[0], values[1]), None)
            i += 1
            if i >= 10:
                break

if __name__ == '__main__':
    PartCJob2.run()

# python partc2.py partc1textoutput.txt > partc2textoutput.txt
