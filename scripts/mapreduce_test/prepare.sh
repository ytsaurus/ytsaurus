#!/bin/sh -eu

echo " #!/bin/sh
./gen_terasort 25000000" > run.sh

rm -f input
touch input
for (( i = START ; i < START + 4000; i++ ))
do
    echo -e "$i\t\t" >> input
done

./mapreduce -server $SERVER -write "$INPUT" -chunksize 1 <input
