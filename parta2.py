from mrjob.job import MRJob
import re
import time
import datetime

class partA2_TimeAnalysis(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                time_epoch = int(fields[6])
                month = time.strftime("%m", time.gmtime(time_epoch))
                year = time.strftime("%Y", time.gmtime(time_epoch))
                year_month = (year, month)
                transaction_count = 1
                transaction_value = int(fields[3])
                yield (year_month, (transaction_value, transaction_count))
        except:
            pass

    def combiner(self, month, values):
        count = 0
        total = 0
        for value in values:
            count += value[1]
            total += value[0]
        yield (month, (total,count))

    def reducer(self, month, values):
        count = 0
        total = 0
        for value in values:
            count += value[1]
            total += value[0]
        yield (month, total/count)


if __name__ == '__main__':
    partA2_TimeAnalysis.run()

# python parta2.py -r hadoop hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1606730688641_4329/
