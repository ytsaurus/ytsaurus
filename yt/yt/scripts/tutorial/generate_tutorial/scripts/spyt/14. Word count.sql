-- Count the occurrences of each word in a given text
SELECT word, COUNT(word) AS word_count -- Selects each unique word and counts its occurrences
FROM (
  -- Splits the provided text into individual words based on whitespace, lower and transforms them into rows
  SELECT EXPLODE(SPLIT(LOWER('The quick brown fox jumps over the lazy dog the quick brown fox'), ' ')) AS word
)
GROUP BY word
ORDER BY word_count DESC;
