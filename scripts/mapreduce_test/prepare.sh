#!/bin/sh -eu

# export server variable
source ./server.sh

make
echo " #!/bin/sh
./gen_terasort 5000000" > run.sh

touch input
for (( i = 1 ; i <= 2000; i++ ))
do
    echo -e "$i\t\t" >> input
done

./mapreduce -server $SERVER -write speed_test/input -chunksize 1 <input

./mapreduce -server $SERVER -map ./run.sh -file run.sh -file gen_terasort -src speed_test/input -dst speed_test/output -subkey -jobcount 2000 -threadcount 10
