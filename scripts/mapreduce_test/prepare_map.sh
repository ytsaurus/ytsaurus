#!/bin/sh -eu

echo "./mapreduce -server $SERVER -map ./run.sh -file run.sh -file gen_terasort -src "$INPUT" -dst "$OUTPUT" -subkey -jobcount 4000 -threadcount 16"
./mapreduce -server $SERVER -map ./run.sh -file run.sh -file gen_terasort -src "$INPUT" -dst "$OUTPUT" -subkey -jobcount 4000 -threadcount 16
