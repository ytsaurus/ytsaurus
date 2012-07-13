#!/bin/sh -eu

$MAPREDUCE -server $SERVER -map ./run.sh -file run.sh -file gen_terasort -src "$INPUT" -dst "$OUTPUT" -subkey -jobcount 4000 -threadcount 16 -opt cpu.intensive.mode=1
