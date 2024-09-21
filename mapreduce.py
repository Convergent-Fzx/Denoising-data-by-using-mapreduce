from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import re

tau = jobconf_from_env('myjob.settings.tau')
class proj1(MRJob): 
    

    def steps(self):
        JOBCONF = {
                       'stream.num.map.output.key.fields': 2,
                       'mapreduce.map.output.key.field.separator': ',',
                       'mapreduce.partition.keypartitioner.options': '-k1,1',
                       'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
                       'mapreduce.partition.keycomparator.options': '-k1,1 -k2,2nr '
                   }
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer,
                   
                   jobconf= JOBCONF)
        ]
        
    
    def mapper(self, _, line):    
        pattern = r'\s+'
        words = re.split(pattern,line)
        if len(words) == 8:
            city = words[3]
            year = int(words[6])
            try:
                temp = float(words[7])
            except ValueError:
                return
            xinxi = (city,int(year))
            
            yield xinxi,(temp,1)
            if 1995 <= int(year) <= 2020:
                yield (city,99999),(temp,1)
      
    def combiner(self,key,values):
        total_temp = 0
        total_count = 0
        for temp, count in values:
            total_temp += temp
            total_count += count
        yield key, (total_temp, total_count)

    def reducer(self, key, values):
        city, year = key
        total_temp = 0
        total_count = 0
        
        
        for temp, count in values:
            total_temp += temp
            total_count += count
        
        if year == 99999:
            overall_avg_temp = ((total_temp / total_count) - 32) * 5 / 9
            self.overall_avg_temp = overall_avg_temp
        else:
            avg_temp = ((total_temp / total_count) - 32) * 5 / 9
            if self.overall_avg_temp is not None and avg_temp > self.overall_avg_temp + float(tau):
                difference = avg_temp - self.overall_avg_temp
                yield city, f"{year},{difference}"


if __name__ == '__main__':
    proj1.run()