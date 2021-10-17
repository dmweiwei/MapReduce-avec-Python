import os
import re

import mrs
import glob


class WordCount(mrs.MapReduce):

    def get_file_list(self):
        file_list = glob.glob(self.args[0] + "/*.txt")
        return file_list

    def input_data(self, job):
        file_list = self.get_file_list()
        return job.file_data(file_list)

    def map(self, key, value):
        # wordlist = re.split('\W+', value)
        wordlist = re.split('[^a-zA-Z]+', value)
        for word in wordlist: # value.split():
            word = word.strip()
            if word:
                yield word.lower(), 1

    def reduce(self, key, values):

        yield key, sum(list(values))


if __name__ == '__main__':
    mrs.main(WordCount)

