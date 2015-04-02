#include <stdlib.h>

void to_lower(
    char** result,
    int* result_length,
    char* string,
    int length)
{
    char* lower_string = (char*)malloc(length * sizeof(char));
    for (int i = 0; i < length; i++) {
        if (65 <= string[i] && string[i] <= 90) {
            lower_string[i] = string[i] + 32;
        } else {
            lower_string[i] = string[i];
        }
    }
    *result = lower_string;
    *result_length = length;
}
