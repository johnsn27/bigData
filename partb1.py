from mrjob.job import MRJob
import re
import time
import datetime

class PartB1(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if (len(fields) == 7):
                to_address = fields[2]
                value = int(fields[3])
                yield(to_address, value)
        except:
            pass

    def reducer(self, address, value):
        yield (address, sum(value))

if __name__ == '__main__':
    PartB1.run()

#  python partb1.py -r hadoop --output-dir partb1output --no-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_0916/
