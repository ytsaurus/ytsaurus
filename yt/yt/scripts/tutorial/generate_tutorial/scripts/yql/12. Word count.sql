-- Count the occurrences of each word in a given text
SELECT word, COUNT(word) AS word_count -- Selects each unique word and counts its occurrences
FROM (
  -- Splits the provided text into a list of individual words based on whitespace and lower them
  SELECT Unicode::SplitToList(Unicode::ToLower('Het is beter ham zonder mosterd dan mosterd zonder ham'), ' ') AS word
) FLATTEN LIST BY word -- Expanding the list into separate rows
GROUP BY word
ORDER BY word_count DESC, word;
