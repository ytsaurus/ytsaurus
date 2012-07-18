#!/bin/sh -eu

JOBCOUNT=1000
JOB_RECORDS=10000000

if [ "$SYSTEM" = "mapreduce" ]; then
    rm -f input
    touch input
    for (( i = START ; i < START + $JOBCOUNT; i++ ))
    do
        echo -e "$i\t" >> input
    done

    $MAPREDUCE -server $SERVER -write "$INPUT" <input
    $MAPREDUCE -server $SERVER -map "./gen_terasort $JOB_RECORDS $SYSTEM" -file gen_terasort \
        -src "$INPUT" -dst "$OUTPUT" -jobcount $JOBCOUNT -threadcount 16 -opt cpu.intensive.mode=1

elif [ "$SYSTEM" = "yt" ]; then
    echo -e "
import config
import yt
config.DEFAULT_PROXY='$SERVER'
config.DEFAULT_FORMAT=yt.DsvFormat()

input = '//home/ignat/' + '$INPUT'
output = '//home/ignat/' + '$OUTPUT'
yt.write_table(input, ['k=%d\\\n' % i for i in xrange($JOBCOUNT)])
yt.create_table(output)
yt.set_attribute(output, 'channels', '[[\\\"k\\\", \\\"v\\\"]]')
spec = {'job_count': $JOBCOUNT,
        'locality_timeout': 0}
yt.run_map('./gen_terasort $JOB_RECORDS $SYSTEM', input, yt.Table(output, append=True), files='gen_terasort', spec=spec)

" >gen.py
    python gen.py

fi
