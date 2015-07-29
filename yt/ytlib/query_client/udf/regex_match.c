#include <regex.h>
#include <stdio.h>

#include <yt_udf.h>

// Searches for a match for 'regex' at the beginning of 'input'.
// Similar to Python's 're.match()'.
char regex_match(
    TExecutionContext* context,
    char* input,
    int input_len,
    char* regex,
    int regex_len)
{
    char* null_term_input = AllocateBytes(context, input_len + 1);
    char* null_term_regex = AllocateBytes(context, regex_len + 1);

    for (int i = 0; i < input_len; i++) {
        null_term_input[i] = input[i];
    }
    null_term_input[input_len] = 0;

    for (int i = 0; i < regex_len; i++) {
        null_term_regex[i] = regex[i];
    }
    null_term_regex[regex_len] = 0;

    regex_t regex_object;

    int error = regcomp(&regex_object, null_term_regex, REG_EXTENDED);

    if (error != 0) {
        int bufflen = 200;
        char errbuf[bufflen];
        regerror(error, &regex_object, errbuf, bufflen);
        printf("Regex compilation error: %s\n", errbuf);
    }

    regmatch_t match;
    error = regexec(&regex_object, null_term_input, 1, &match, 0);
    regfree(&regex_object);

    return error == 0 && match.rm_so == 0;
}
