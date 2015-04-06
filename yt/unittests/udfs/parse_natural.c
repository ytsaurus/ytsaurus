#include <unversioned_value.h>

unsigned long parse_natural(TExecutionContext* context, const char* string, int length)
{
    unsigned long result = 0;
    for (int i = 0; i < length; i++) {
        result *= 10;
        int digit = string[i] - 48;
        result += digit;
    }
    return result;
}
