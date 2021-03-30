from mrjob.job import MRJob
import re
import time
import statistics
from mrjob.step import MRStep
import json
from datetime import datetime

class PartDScamAnalysis2(MRJob):
    sector = {}

    def mapper_join_init(self):
        with open('scams.json','r') as f:
            file = json.load(f)
        for key in file['result']:
            value = file['result'][key]['category']
            self.sector[key] = value

    def mapper_repl_join(self, _, line):
        fields = line.split(",")
        try:
            if((len(fields)==7)):
                to_address = fields[2]
                if to_address in self.sector:
                    time_epoch = int(fields[6])
                    year = time.strftime("%Y",time.localtime(time_epoch))
                    month = time.strftime("%m",time.localtime(time_epoch))
                    year_month = (year, month)
                    value = int(fields[3])
                    category = self.sector[to_address]
                    yield( (category,year_month), value)
        except:
            pass

    def reducer_sum(self,category_year_month,values):
        yield(category_year_month,sum(values))

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                         mapper=self.mapper_repl_join),
                 MRStep(reducer=self.reducer_sum)]


if __name__ == '__main__':
    PartDScamAnalysis2.run()

# python partdScams2.py -r hadoop --file scams.json --output-dir partdScamsOutput2 --no-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions

# python partdGasGuzzlers2.py -r hadoop --file partb3textoutput.txt --output-dir partdGasGuzzlerOutput2 --no-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions

# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_7602/
# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_7608/
