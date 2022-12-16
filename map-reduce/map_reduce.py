from mrjob.job import MRJob
from mrjob.step import MRStep

class MR_ShowGenrer_Frequency(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer = self.reducer
            )
        ]
    def mapper(self, _, line):
        fields = line.split("|")

        def has_number(string):
            return any(item.isdigit() for item in string)

        if len(fields) >= 7:
            genrers = str(fields[8].split(','))
            t_genrer = genrers.translate({ord(i): None for i in '[]"\" '''})
            split_genrer = t_genrer.split(',')
            for item in split_genrer:
                cleaned_genrer = item.translate({ord(i): None for i in "'"})
                if not has_number(cleaned_genrer):
                    if not cleaned_genrer == "":
                        yield cleaned_genrer, 1

    def reducer(self, key, values):
        yield key, sum(values) 
if __name__ == "__main__":
    MR_ShowGenrer_Frequency.run() 
