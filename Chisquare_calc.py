import json
#import re
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol
from mrjob.step import MRStep


class Chisquare_job(MRJob):

    # initializing global parameters and variable
    Total_reviews = 0
    Reviews_per_cat = {}
    FILES = ['counters.txt']
    OUTPUT_PROTOCOL = RawProtocol

    def steps(self):

        return [
            MRStep(mapper=self.chisquare_per_word,
                   mapper_init = self.mapper_intitialize,
                   reducer=self.top_75_per_cat),
            MRStep(reducer = self.sort_alphabet)
        ]

    # mapper
    # gets word and dictionary with counts per category
    # calculates chisquare value for each word
    # returns word as key, chisquare value and category as value

    def chisquare_per_word(self, _, line):

        # to deal with empty line produced at the end of output of previous job
        if len(line) == 0:
            return

        # extracting word and dictionary of counts per category
        word, cat_dict = str(line).split(' ',1)
        cat_dict = json.loads(str(cat_dict).replace("'",'"'))


        # calculating all the parts of the chisquare value
        for cat in cat_dict.keys():
            A = cat_dict[cat]
            C = self.Reviews_per_cat[cat] - A
            B = sum(cat_dict.values()) - A
            D = self.Total_reviews -(A+B+C)

        # calculating chisquare value
            chisq = (self.Total_reviews*(A*D - B*C)**2)/((A+B)*(A+C)*(B+D)*(C+D))

        # yielding category as key, word and chisquare value as value
            yield cat, [word,chisq]



    # mapper initialisation function
    # reads in the counters from the runner file
    def mapper_intitialize(self):
        with open("counters.txt") as reader:
            self.Total_reviews= int(reader.readline())
            self.Reviews_per_cat = json.loads(reader.readline().replace("'",'"'))


    # first reducer
    # takes the keys and values from the mapper
    # returns the list of word-chisquare pairs sorted in descending order by chisquare value with category as key
    def top_75_per_cat(self, cat, wc_list):

        # sorting list of word - chisquare values by chisquare for each category
        # only yield the 75 words with the highest chisquare per category
        # yield with Null-key, to be able to access all the words in the next step

        yield None, [cat,sorted(list(wc_list), key=lambda x: int(x[1]),reverse=True)[0:75]]

    # second reducer
    # takes all the sorted top 25 chisquare lists from the last step
    # outputs for each category the 75 words with highest chisquare value and the chisquare value as a string
    # outputs final line as string with all words in alphabetical order

    def sort_alphabet(self, _,top75perCat):
        sorted_top75 = sorted(list(top75perCat), key=lambda x: x[0])
        wordset = set()
        categorystring = ""
        finalstring = ""

        for entry in sorted_top75:

            categorystring = ""

            # putting together the output string for each category in the right format
            for pair in entry[1]:
                categorystring = categorystring + " " + pair[0] +":" + str(pair[1])
                wordset.add(pair[0])

            # for each category yield the category name and the output string
            yield "<" + str(entry[0]) + ">", categorystring

        # loop to create the final line of all the words separated by tabs
        for element in sorted(list(wordset)):
            finalstring = finalstring +" " + element


        yield None, finalstring




if __name__ == '__main__':
    Chisquare_job.run()
