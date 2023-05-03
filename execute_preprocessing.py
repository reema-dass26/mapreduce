from Preprocessing_job import Preprocessing_job

# runner file
# executes the preprocessing job
# saves the counters for reviews and reviews per category and writes them to a text file


pj = Preprocessing_job()

with pj.make_runner() as runner:
    runner.run()

    # get the counters out of the runner file
    Total_reviews = runner.counters()[0]['n_reviews']['counter']
    Reviews_per_cat = runner.counters()[0]['rev_per_cat']

    # Write them to a text file
    file = open("counters.txt", "w")
    file.write(str(Total_reviews)+"\n")
    file.write(str(Reviews_per_cat))
    file.close()

    # Print key-value pairs to  output file
    for key, value in pj.parse_output(runner.cat_output()):
        print(key, value, "\n", end='')