from mrjob.job import MRJob
import re
import time
import statistics
from mrjob.step import MRStep
from datetime import datetime

class PartDGazGuzzlersJob2(MRJob):
    sector = {}

    def mapper_join_init(self):
        with open("partb3textoutput.txt") as f:
            for line in f:
                address_value = line.split("\t")
                fields = address_value[0].split(",")
                address = fields[0][2:-1]
                self.sector[address] = fields[1]

    def mapper_repl_join(self, _, line):

        fields = line.split(",")
        try:
            if((len(fields)==7)):
                to_address = fields[2]
                if to_address in self.sector:
                    transaction_value = int(fields[3])
                    time_epoch = int(fields[6])
                    year = time.strftime("%Y",time.localtime(time_epoch))
                    month = time.strftime("%m",time.localtime(time_epoch))
                    year_month = (year, month)
                    address_date = (to_address,year_month)
                    yield (address_date, transaction_value)
        except:
            pass

    def reducer_sum(self,address_date,transaction_value):
        yield(address_date,statistics.mean(transaction_value))

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                         mapper=self.mapper_repl_join),
                 MRStep(reducer=self.reducer_sum)]


if __name__ == '__main__':
    PartDGazGuzzlersJob2.run()

# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_8473/
# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_8498/

# python partdGasGuzzlers2.py -r hadoop --file partb3textoutput.txt --output-dir partdGasGuzzlerOutput2 --no-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
