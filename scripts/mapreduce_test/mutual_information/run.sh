#!/bin/sh -eux

# 1Tb = 10^12 = (word_size + space) 10 * (words in record) 10000 * (jobs) 500 * (records) 20000

export JOBCOUNT=5

export MAPREDUCE="../mapreduce -server w301.hdp.yandex.net:8013 -jobcount $JOBCOUNT -subkey"

export TEMP_TABLE="ignat/temp_table"
export DATA_TABLE="ignat/random_texts"
export WORD_COUNT_TABLE="ignat/word_count"
export PMI_TABLE="ignat/pmi"

WORDS_IN_RECORD=1000
RECORD_PER_JOB=2000
WORD_COUNT=`echo "$WORDS_IN_RECORD * $RECORD_PER_JOB * $JOBCOUNT" | bc`
PAIRS_COUNT=`echo "($WORDS_IN_RECORD - 1) * $RECORD_PER_JOB * $JOBCOUNT" | bc`
DICTIONARY_SIZE=`echo "sqrt($WORD_COUNT)" | bc`

./prepare_data/make_dictionary.py 9 $DICTIONARY_SIZE > dict

rm -f input
touch input
for (( i = 0 ; i < $JOBCOUNT; i++ ))
do
    echo -e "$i\t\t" >> input
done

time $MAPREDUCE -write "$TEMP_TABLE" <input
time $MAPREDUCE -map "PYTHONPATH=. ./map.py dict $RECORD_PER_JOB $WORDS_IN_RECORD" -src "$TEMP_TABLE" -dst "$DATA_TABLE" -file "dict" -file "./prepare_data/map.py" -file "./prepare_data/dictionary.py" -opt cpu.intensive.mode=1

time $MAPREDUCE -map "./split.py" -src "$DATA_TABLE" -dst "$TEMP_TABLE" -file "count_wc/split.py"
time $MAPREDUCE -reduce "./collect.py" -src "$TEMP_TABLE" -dst "$WORD_COUNT_TABLE" -file "count_wc/collect.py"

time $MAPREDUCE -map "./second_word_to_subkey.py" -src "$WORD_COUNT_TABLE" -dst "${PMI_TABLE}_1" -file "count_pmi/second_word_to_subkey.py"
time $MAPREDUCE -reduce "./calculate.py $WORD_COUNT $PAIRS_COUNT 1" -src "${PMI_TABLE}_1" -dst "${PMI_TABLE}_2" -file "count_pmi/calculate.py"
time $MAPREDUCE -reduce "./calculate.py $WORD_COUNT $PAIRS_COUNT 0" -src "${PMI_TABLE}_2" -dst "${PMI_TABLE}_res" -file "count_pmi/calculate.py"

rm -f dict input
