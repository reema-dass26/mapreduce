import json
import re
from mrjob.job import MRJob
from mrjob.step import MRStep




class Preprocessing_job(MRJob):
    FILES = ['stopwords.txt']

    def steps(self):

        return [
            MRStep(mapper=self.get_word_cat_pairs,
                   combiner=self.catcount_per_word,
                   reducer=self.catcount_reducer)
        ]
    # mapper function: gets line of amazon review dataset
    # makes global count of number of reviews per category and in total
    # performs stop word removal and regex tokenization
    # yields key-value pair of form (word,category) for each word that is not a stop word and has len>1
    def get_word_cat_pairs(self, _, line):

        #reading in stopword list and eliminating duplicates by converting to set
        stopwords = set(i.strip() for i in open('stopwords.txt'))

        # loading next line of json file, extracting category and text of review
        currentline = json.loads(line)
        category, revtext = currentline['category'], currentline['reviewText']
        # transforming reviewtext to lower case
        rawtext = revtext.lower()

        # counter for mapper calls: gives us total number of reviews
        self.increment_counter('n_reviews', 'counter')

        # counts mapper calls per category: gives us number of reviews per category
        self.increment_counter('rev_per_cat', category)


        # splits the reviewtext by all characters except letters and <> (so all specified in exercise description)
        rawtext = re.split('[^a-zA-Z<>^|]+', rawtext)

        # converting wordlist to set to remove duplicates
        wordset = set(rawtext)

        # a key-value pair of the form (word,category) is yielded for each word that is not a stopword and has length > 1
        for word in wordset:
            if len(word) > 1 and word not in stopwords:
                yield word, category

    # combiner
    # gets words as keys and for each word a list of all categories it appears
    # returns key- value pairs with words as keys and as value a dictionary with all the categories the
    # word appears in as keys and how often the word appeared
    def catcount_per_word(self, word, cats):
        word_cat_dict = {}

        for cat in list(cats):
            if cat in word_cat_dict.keys():
                word_cat_dict[cat] += 1
            else:
                word_cat_dict[cat] = 1

        yield word, word_cat_dict

    # second reducer
    # gets words as keys and as values (potentially multiple) dictionaries with the counts from the previous step
    # merges the dictionaries into a single one
    # returns word as key and the merged dictionary as value

    def catcount_reducer(self, word, word_count_dicts):

        word_count = {}


        for word_dic in word_count_dicts:
            for key,value in word_dic.items():
                if key in word_count.keys():
                    word_count[key] += value
                else:
                    word_count[key] = value



        yield word, word_count


if __name__ == '__main__':
    Preprocessing_job.run()
