from mrjob.job import MRJob
import re
import time
import statistics
from datetime import datetime

class PartDGazGuzzlersJob1(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if((len(fields)==7)):
                date = time.localtime(int(fields[6]))
                year = date[0]
                month = date[1]
                gas_price = int(fields[5])
                year_month = (year, month)
                yield(year_month,gas_price)
        except:
            pass


    def reducer(self, year_month, gas_price):
        yield(year_month,statistics.mean(gas_price))



if __name__ == '__main__':
    PartDGazGuzzlersJob1.run()

# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_6870/


# python partdGasGuzzlers.py -r hadoop --output-dir partdGasGuzzlerOutput --no-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions

