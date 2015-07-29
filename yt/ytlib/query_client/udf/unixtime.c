#define _XOPEN_SOURCE

#include <time.h>
#include <stdio.h>

#include <yt_udf.h>

int64_t unixtime(
    TExecutionContext* context,
    char* date,
    int date_len,
    char* format,
    int format_len)
{
    char* null_term_date = AllocateBytes(context, date_len + 1);
    char* null_term_format = AllocateBytes(context, format_len + 1);

    for (int i = 0; i < date_len; i++) {
        null_term_date[i] = date[i];
    }
    null_term_date[date_len] = 0;

    for (int i = 0; i < format_len; i++) {
        null_term_format[i] = format[i];
    }
    null_term_format[format_len] = 0;

    struct tm time;
    strptime(null_term_date, null_term_format, &time);
    return (int64_t)mktime(&time);
}
