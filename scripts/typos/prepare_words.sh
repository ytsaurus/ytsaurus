#!/bin/bash

rm all_words all_words_uniqed; 
for f in `find ../../ | fgrep -e .cpp -e .h -e .py` ; 
do 
    cat $f \
        | sed -s "s/[][ \t~\`\"'_\$!#@;<>\/\\()&|^0123456789,*:%=+\.{}-]/\n/g" \
        | sed -s "s/[ETNI]\([A-Z]\)/\1/g" \
        | sed -s "s/\([a-z]\)\([A-Z]\)/\1\n\2/g" \
        | sed '/^$/d' \
        | tr A-Z a-z \
        | sed -s "/^[a-z]\{0,4\}$/d" \
        >>all_words ; 
    echo $f; 
done; 
cat all_words | sort | uniq -c | sort -n >all_words_uniqed;
ispell -a <all_words_uniqed | grep -a ^\& >poss_errors
