#include <udf_helpers.h>
#include <ctype.h>

void to_lower(
    TExecutionContext* context,
    char** result,
    int* result_len,
    char* s,
    int s_len)
{
    char* lower_string = AllocateBytes(context, s_len * sizeof(char));
    for (int i = 0; i < s_len; i++) {
        lower_string[i] = tolower(s[i]);
    }

    *result = lower_string;
    *result_len = s_len;
}
