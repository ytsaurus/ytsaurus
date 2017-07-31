#!/bin/bash

git ls-files -z | while read -d $'\0' file
do
    case "$file" in
        contrib/*|yt/contrib/*)
            continue
            ;;
        *.cpp)
            ;;
        *.h)
            if ! fgrep -q '#pragma once' "$file"; then
                perl -pe 'print "#pragma once\n\n" if $. == 1' -i "$file"
            fi
            ;;
        *)
            continue
            ;;
    esac
    perl -pe 's/\s+$/\n/' -i "$file"  # remove trailing spaces
    perl -pe 's/\t/    /g' -i "$file"  # replace tabs with spaces
    perl -pe '$_ .= "\n" unless ($_ =~ /\n/)' -i "$file"  # add a terminating newline
    perl -pe '$_ = "/" x 80 . "\n" if $_ =~ /^\s*\/+\s*$/' -i "$file"  # unify slashes
done
