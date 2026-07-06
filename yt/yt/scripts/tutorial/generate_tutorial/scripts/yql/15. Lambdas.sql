-- A named unicode text literal (constant) which used below.
$separator = ", "u;

-- $Join will store a lambda and $list is an argument.
-- This demonstrates the simplest syntax with a single function
-- whose first argument is taken from the lambda argument.
-- and the second is captured from outside.
-- ListConcat concatenates items of a list given as first argument into a string,
-- using a delimiter provided as the second argument.
$Join = ($list) -> (ListConcat($list, $separator));

-- $Cut will store a lambda with two arguments
-- This demonstrates a more complicated syntax with a code block
-- and 'return' statement inside.
$Cut = ($text, $limit) -> {
    -- Named expressions can be declared inside a lambda.
    $dots = "..."u;

    $cut_from = Unicode::RFind( -- Unicode::RFind searches for the first position
                                -- of a substring in the text starting from the end.
     -- Unicode::Substring extracts a substring from the text without allocating or copying memory.
        Unicode::Substring($text, 0, $limit - Unicode::GetLength($dots)),
        $separator
     -- Unicode::GetLength returns the length of a unicode string in characters.
    ) + Unicode::GetLength($separator);

    -- Returns its second or third argument depending on the condition in the first argument.
    return if(
        Unicode::GetLength($text) > $limit, -- Checks a length of the text and
        Unicode::Substring($text, 0, $cut_from) || $dots, -- returns the truncated text supplemented with dots
        $text -- or the original text as is.
    );
};

SELECT
    origin, -- 'origin' is an alias which defined below in GROUP BY
    $Cut($Join(AGGREGATE_LIST(name)), 200) AS all_names,
        -- AGGREGATE_LIST is an aggregation function that returns all passed values as list
    $Join(AGGREGATE_LIST(name, 4)) AS any_four_names,
        -- The second argument of AGGREGATE_LIST is the limit of elements in the resulting list
    $Join(BOTTOM(name, 3)) AS first_3_names,
        -- BOTTOM returns the requested number of smallest values ​​of a column as a list.
    $Join(TOP(name, 3)) AS last_3_names, -- aggregation function that
        -- TOP returns the requested number of largest values ​​of a column as a list.
FROM `$nomenclature`
GROUP BY
   Yson::AsString(meta_data.origin) AS origin
        -- GROUP BY can be performed on arbitrary expression and its
        -- result is available in SELECT via alias specified with AS
ORDER BY origin
;

