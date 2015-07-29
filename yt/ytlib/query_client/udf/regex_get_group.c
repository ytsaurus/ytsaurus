#include <regex.h>
#include <string.h>

#include <yt_udf.h>

void regex_get_group(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* input,
    TUnversionedValue* regex,
    TUnversionedValue* group_index)
{
    if (group_index->Type == Null || regex->Type == Null || input->Type == Null) {
        result->Type = Null;
        return;
    }


    char* null_term_input = AllocatePermanentBytes(context, input->Length + 1);
    memcpy(null_term_input, input->Data.String, input->Length);
    null_term_input[input->Length] = 0;

    char* null_term_regex = AllocateBytes(context, regex->Length + 1);
    memcpy(null_term_regex, regex->Data.String, regex->Length);
    null_term_regex[regex->Length] = 0;

    regex_t regex_object;
    int error = regcomp(&regex_object, null_term_regex, REG_EXTENDED);

    if (error != 0) {
        int bufflen = 200;
        char errbuf[bufflen];
        regerror(error, &regex_object, errbuf, bufflen);
        ThrowException(errbuf);
    }

    uint64_t index = group_index->Data.Uint64;

    regmatch_t matches[index + 1];
    error = regexec(
        &regex_object,
        null_term_input,
        index + 1,
        matches,
        0);

    regfree(&regex_object);

    if (error == 0 && matches[index].rm_so != -1) {
        result->Type = String;
        result->Length = matches[index].rm_eo - matches[index].rm_so;
        result->Data.String = null_term_input + matches[index].rm_so;
    } else {
        result->Type = Null;
    }
}
