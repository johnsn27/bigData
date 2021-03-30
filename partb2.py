from mrjob.job import MRJob

class PartBJob2(MRJob):

    def mapper(self, _, line):
        try:
            contract_fields = line.split(',')
            job1_fields = line.split('\t')
            if len(contract_fields)==5:
                fields = line.split(',')
                join_key = fields[0]
                join_value = int(fields[3])
                yield (join_key, (join_value,1))


            elif len(job1_fields)==2:
                fields = line.split('\t')
                join_key = fields[0]
                join_value = int(fields[1])
                join_key = join_key.replace('"', '')
                yield (join_key,(join_value,2))
        except:
            pass

    def reducer(self, address, values):

        contracts = ''
        total_value = ''

        for value in values:
            if value[1] == 1:
                contracts = value[0]
            elif value[1] == 2:
                contracts = ''
                total_value = value[0]

        if contracts != '':
            yield (address, total_value)

if __name__ == '__main__':
    PartBJob2.run()

# python partb2.py -r hadoop --output-dir partb2output --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts partb1textoutput.txt

# http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_1053/
