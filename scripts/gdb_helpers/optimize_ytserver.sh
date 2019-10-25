if [ "$#" != "1" ]; then
    echo -ne "Usage: ./optimize_ytserver.sh <path_to_ytserver>\n\nConvenient form: ./optimize_ytserver.sh \`which ytserver\`\n"
    exit 1
fi
if ! [ -e $1 ]; then
    echo -ne "$1 does not exist"
    exit 1
fi
path=$(dirname $1)
echo Processing ytserver from $path
echo Building gdb-index...
gdb $1 -batch --ex "save gdb-index $path"
echo Adding index to $1...
objcopy --add-section .gdb_index=$1.gdb-index --set-section-flags .gdb_index=readonly $1 $1
echo Done!
