---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/text_processing.md
sourcePath: yql-product/udf/list/text_processing.md
---
# Text Processing

UDF for text processing.

## Tokenizer
### Description
```yql
TextProcessing::MakeTokenizer(
    Lowercasing:Bool?, -- whether to convert tokens to lower case after tokenization
    Lemmatizing:Bool?, -- whether to lemmatize tokens after tokenization
    NumberProcessPolicy:String?, -- numeric token processing strategy.
        Possible values:
        -- Skip — skip numeric tokens.
        -- LeaveAsIs — leave numeric tokens as is.
        -- Replace — replace numeric tokens with one special token
            specified by NumberToken.
    NumberToken:String?, -- the token that will be substituted for all numeric tokens,
        if NumberProcessPolicy == Replace
    SeparatorType:String?, -- tokenization method. Possible values:
        -- ByDelimiter — regular split by delimiter
        -- BySense — tokenization that tries to split the string into tokens by meaning
    Delimiter:String?, -- used only if SeparatorType == ByDelimiter
    SplitBySet:Bool?, -- if True, each separate character in the Delimiter parameter
        is treated as a separate delimiter. So, the string can be splitted
        using several delimiters.
    SkipEmpty:Bool?, -- if True, empty tokens are skipped.
    TokenTypes:List<String>?, -- list of token types that will be kept after tokenization.
        -- Possible values: Word, Number, Punctuation, SentenceBreak, ParagraphBreak, Unknown
        -- used only if SeparatorType == BySense
    SubTokensPolicy:String?, -- Possible values:
        -- SingleToken — all subtokens are treated as one token.
        -- SeveralTokens — each subtoken is treated as a separate token.
        -- Used only if SeparatorType == BySense
    Languages:List<String>? -- List of languages.
        -- Used only if Lemmatizing == True;
) -> List<Struct<
    Token:String, -- token
    Type:String -- token type
>>
```

Example:

```yql
$simpleTokenizer = TextProcessing::MakeTokenizer();
$tokenizer = TextProcessing::MakeTokenizer(
    True as Lowercasing,
    True as Lemmatizing,
    "BySense" as SeparatorType,
    AsList("Word", "Number") as TokenTypes
);

SELECT
    $simpleTokenizer(value) AS simple,
    $tokenizer(value) AS tok
FROM Input
```

<!--Another [example](https://cluster-name.yql/Operations/X5seqxpqv1DWZmHFxj3OtQx6yGPCQJPDV_ipr__dfIQ=).-->
