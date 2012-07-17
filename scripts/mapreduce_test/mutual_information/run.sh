#!/bin/sh -eux

# 1Tb = 10^12 = (word_size + space) 10 * (words in record) 10000 * (jobs) 500 * (records) 20000

export JOBCOUNT=5

export MAPREDUCE="../mapreduce -server w301.hdp.yandex.net:8013 -jobcount $JOBCOUNT"

export TEMP_TABLE="ignat/temp_table"
export DATA="ignat/random_texts"
export WORD_COUNT="ignat/word_count"
export PMI="ignat/pmi"

WORDS_IN_RECORD=1000
RECORD_PER_JOB=2000
WORD_COUNT=`echo "$WORDS_IN_RECORD * $RECORD_PER_JOB * $JOBCOUNT" | bc`
PAIRS_COUNT=`echo "($WORDS_IN_RECORD - 1) * $RECORD_PER_JOB * $JOBCOUNT" | bc`
DICTIONARY_SIZE=`echo "$WORD_COUNT ^ 0.5" | bc`

./prepare_data/make_dictionary.py 9 $DICTIONARY_SIZE > dict

rm -f input
touch input
for (( i = 0 ; i < $JOBCOUNT; i++ ))
do
    echo -e "$i\t\t" >> input
done

$MAPREDUCE -write "$TEMP_TABLE" <input
$MAPREDUCE -map "PYTHONPATH=. ./map.py dict 10000 20000" -src "$TEMP_TABLE" -dst "$DATA" -file "dict" -file "./prepare_data/map.py" -file "./prepare_data/dictionary.py" -opt cpu.intensive.mode=1

$MAPREDUCE -map "./split.py" -src "$DATA" -dst "$TEMP_TABLE" -file "count_wc/split.py" 
$MAPREDUCE -map "./collect.py" -src "$TEMP_TABLE" -dst "$WORD_COUNT" -file "count_wc/collect.py" 

$MAPREDUCE -map "./second_word_to_subkey.py" -src "$WORD_COUNT" -dst "${PMI}_1" -file "count_pmi/second_word_to_subkey.py" 
$MAPREDUCE -map "./calculate.py $WORD_COUNT $PAIRS_COUNT 0" -src "${PMI}_1" -dst "${PMI}_2" -file "count_pmi/calculate.py" 
$MAPREDUCE -map "./calculate.py $WORD_COUNT $PAIRS_COUNT 1" -src "${PMI}_2" -dst "${PMI}_res" -file "count_pmi/calculate.py" 
