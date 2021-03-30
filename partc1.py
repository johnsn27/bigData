from mrjob.job import MRJob

class PartCJob1(MRJob):

    def mapper(self, _, line):
        try:
            blocks_fields = line.split(',')
            if len(blocks_fields) == 9:
                miners = blocks_fields[2]
                size = int(blocks_fields[4])
                yield (miners, size)

        except:
            pass

    def combiner(self, miners, size):
        yield (miners, sum(size))

    def reducer(self, miners, size):
        yield (miners, sum(size))

if __name__ == '__main__':
    PartCJob1.run()

#  python partc1.py -r hadoop --output-dir partc1output --no-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/blocks

# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_3127/
