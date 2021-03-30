from mrjob.job import MRJob
import re
import time
import datetime

class partA_TimeAnalysis(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                time_epoch = int(fields[6])
                month = time.strftime("%m", time.gmtime(time_epoch))
                year = time.strftime("%Y", time.gmtime(time_epoch))
                year_month = (year, month)
                transaction_count = 1
                yield year_month, transaction_count
        except:
            pass

    def reducer(self, month, transaction_count):
        yield month, sum(transaction_count)


if __name__ == '__main__':
    partA_TimeAnalysis.run()

# python parta.py -r hadoop hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1606730688641_4149/
