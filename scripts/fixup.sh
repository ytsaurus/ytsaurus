#!/bin/bash
# This scripts adapts some style checks from Arcadia.

git ls-files -z | while read -d $'\0' file
do
    case "$file" in
        contrib/*|yt/contrib/*)
            continue
            ;;
        yt/ytlib/query_client/lexer.cpp)
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
    perl -pe 'if ($_ =~ /^(\s*)\/\/\/\/\/+\s*$/) { $_ = $1 . "/" x 80 . "\n" }' -i "$file"  # unify slashes
done
