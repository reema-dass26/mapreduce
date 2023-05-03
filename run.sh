INPUT_PATH=hdfs:///user/pknees/amazon-reviews/full/reviewscombined.json

python3 execute_preprocessing.py -r hadoop $INPUT_PATH > intermediate.txt

python3 Chisquare_calc.py -r hadoop intermediate.txt > final_out.txt
