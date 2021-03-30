from mrjob.job import MRJob

class PartBJob3(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split('\t')
            if len(fields) == 2:
                address = fields[0]
                value = int(fields[1])
                yield (None, (address, value))
        except:
            pass

    def combiner(self, _, unsorted_values):
        sorted_values = sorted(unsorted_values, reverse=True, key = lambda unsorted_vals:unsorted_vals[1])
        i=0
        for value in sorted_values:
            yield("Top", value)
            i += 1
            if i >= 10:
                break

    def reducer(self, _, unsorted_values):
        sorted_values = sorted(unsorted_values, reverse=True, key = lambda unsorted_vals:unsorted_vals[1])
        i=0
        for value in sorted_values:
            yield("{} - {}".format(value[0], value[1]), None)
            i += 1
            if i >= 10:
                break


if __name__ == '__main__':
   PartBJob3.run()

# python partb3.py partb2textoutput.txt > partb3textoutput.txt