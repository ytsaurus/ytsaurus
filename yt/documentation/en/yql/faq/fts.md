---
vcsPath: yql/docs_yfm/docs/ru/yql-product/faq/fts.md
sourcePath: yql-product/faq/fts.md
---
# Full-text search of queries and operations

## Why don't I find my operations or queries? I know that they should definitely include my search string.
Searching by substrings isn't supported. You can only search words or phrases.


## Where is the system searching for words?
The system indexes the contents of your query (including the comments inside), its header and tags, the operation header, and the contents of attached files. From the contents of your query and files, it takes only several initial kilobytes.


## Do you support forms of words?
To a certain extent, yes. MongoDB search uses stemmed words, so searching lemmas isn't supported.
If the query includes the word **dog**, you can find it by searching **dogs**. However, it can't find texts that include the word **people** by searching **humans**.

Examples of searching stems:

* The stem **chair** will be found for the word **chairs**.
* The system will find the stem **cry** for the word **crying**.


## Which languages are supported?
Two languages are supported at the moment: Russian and English. If the query is in Russian, the system searches in the Russian index. Otherwise, it searches in the English index.

If the query includes Russian and English words both, then

* For operations, the system searches in the Russian index.
* For queries, it searches both indexes and then joins the search results.


## How do I find two words at the same time.
If the search query includes several words, the resulting documents will include at least one of the words, but not necessarily all of them at the same time. This is a logical **OR** JOIN.
It can't join by **AND**, but you can search exact matches of a phrase by enclosing it in double quotes.


## How do I remove irrelevant search results?
If you're trying to find operations by some words, but the search results are cluttered by irrelevant operations, you can use negative words. Put a minus (-) before such words, and the documents that include negative words will be removed from your search results.
{% note warning "Important!" %}

Heads up: Double-check that there's no space between the minus and the word.

{% endnote %}


## Can't find the preposition "in", but I'm sure that it is there.
Some words have been excluded from the index. They are referred to as stop words. Those are simple short words, for example, prepositions, pronouns, and other similar words. They are used very often, so we discard them when building an index. It doesn't make sense to search them; try to use unique words in your search.<br>

Lists of words:

* For [Russian language](https://github.com/mongodb/mongo/blob/master/src/mongo/db/fts/stop_words_russian.txt).
* For [English language](https://github.com/mongodb/mongo/blob/master/src/mongo/db/fts/stop_words_english.txt).


## Can underscores be used for splitting into distinct words?
No underscores can't be used as word separators. <br>Word separators are spaces, parentheses, slashes, quotes, and other similar characters: , `, !, @, #, $, %, ^, &, *, (, ), -, =, +, [,], {, }, |, \, ;, :, ", <, >, ,(comma), .(dot), /, ?.<br>An apostrophe can only be used as a separator in Russian; in English, it's a word part.


## My search fails with the error: Failed loading search results.
Our search is resource-savvy, so in our system, we established a timeout for database searches. We use the timeout to be sure that the excessive load won't affect stability and response time of our system.
To eliminate this error, you should either rerun your query, or edit it to search for the rarest word in your query texts (this will shrink the data scanned).
The server also consumes more resources when you search full phrases or use negative words.


# Examples
| Text | Search query | Found? |
|------------------------------------------------------------------| --- | --- |
| select * from my_table; | from | <span style="color: red;"> no </span> |
| select * from my_table; | table | <span style="color: red;"> no </span> |
| select * from my_table; | my_table | <span style="color: green;"> yes </span> |
| select * from my_table; | my_tables | <span style="color: green;"> yes </span> |
| select * from my_table; | SELECT | <span style="color: green;"> yes </span> |
| select * from my_table; | Selection | <span style="color: green;"> yes </span> |
| flatten my list; | flatten by | <span style="color: green;"> yes </span> |
| flatten my list; | "flatten by" | <span style="color: red;"> no </span> |
| pragma file("a.sql", "https://<cluster-name>/#page=navigation"); | cluster-name | <span style="color: green;"> yes </span> |
