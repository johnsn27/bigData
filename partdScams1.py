from mrjob.job import MRJob
import re
import time
import statistics
from mrjob.step import MRStep
import json
from datetime import datetime

class PartDScamAnalysis1(MRJob):
    sector = {}

    def mapper_join_init(self):
        with open('scams.json','r') as f:
            file = json.load(f)
        for key in file['result']:
            value = file['result'][key]['category']
            self.sector[key] = value

    def mapper_repl_join(self, _, line):
        fields = line.split('\t')
        try:
            if len(fields)==2:
                address = fields[0][1:-1]
                total_value = int(fields[1])
                for key in self.sector:
                    if key == address:
                        category = self.sector[key]
                        yield( category, total_value)
                        break
        except:
            pass

    def reducer_sum(self,category,total_value_lost):
        yield(category,sum(total_value_lost))

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                         mapper=self.mapper_repl_join),
                 MRStep(reducer=self.reducer_sum)]


if __name__ == '__main__':
    PartDScamAnalysis1.run()

# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_7518/
# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_7541/

# python partdScams1.py -r hadoop --file scams.json --output-dir partdScamsOutput1 --no-output partb1textoutput.txt
