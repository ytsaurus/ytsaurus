#include "yt_udf.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

// For "0001-01-01T00:00:00Z".
static const int64_t TimestampMinSeconds = -62135596800LL;
// For "9999-12-31T23:59:59.999999999Z".
static const int64_t TimestampMaxSeconds = 253402300799LL;

static const int MaxFormatLength = 30;
static const int DefaultBufferLength = 128;

static void validate_timestamp_too_small(int64_t timestamp)
{
    if (timestamp < TimestampMinSeconds) {
        char buffer[DefaultBufferLength];
        int length = snprintf(
            buffer,
            sizeof(buffer),
            "Timestamp is smaller than minimal value: got %" PRId64 " < %" PRId64,
            timestamp,
            TimestampMinSeconds);
        buffer[length] = 0;
        ThrowException(buffer);
    }
}

static void validate_timestamp_too_large(int64_t timestamp)
{
    if (timestamp > TimestampMaxSeconds) {
        char buffer[DefaultBufferLength];
        size_t length = snprintf(
            buffer,
            sizeof(buffer),
            "Timestamp is greater than maximal value: got %" PRId64 " > %" PRId64,
            timestamp,
            TimestampMinSeconds);
        buffer[length] = 0;
        ThrowException(buffer);
    }
}

static void validate_timestamp(int64_t timestamp)
{
    validate_timestamp_too_small(timestamp);
    validate_timestamp_too_large(timestamp);
}

static void validate_format_string_length(int length)
{
    if (length > MaxFormatLength) {
        char buffer[DefaultBufferLength];
        int resultLength = snprintf(
            buffer,
            sizeof(buffer),
            "Format string is too long: %d > %d",
            length,
            MaxFormatLength);
        buffer[resultLength] = 0;
        ThrowException(buffer);
    }
}

static void format_timestamp_helper(
    TExpressionContext* context,
    char** result,
    int* result_len,
    int64_t timestamp,
    char* format)
{
    validate_timestamp(timestamp);

    time_t rawtime = timestamp;
    struct tm timeinfo;
    gmtime_r(&rawtime, &timeinfo);

    char buffer[DefaultBufferLength];
    size_t length = strftime(buffer, sizeof(buffer), format, &timeinfo);

    *result = AllocateBytes(context, length);
    strncpy(*result, buffer, length);
    *result_len = length;
}

void format_timestamp(
    TExpressionContext* context,
    char** result,
    int* result_len,
    int64_t timestamp,
    char* format,
    int format_len)
{
    validate_format_string_length(format_len);

    char buffer[MaxFormatLength + 1];
    memcpy(buffer, format, format_len);
    buffer[format_len] = '\0';

    format_timestamp_helper(context, result, result_len, timestamp, buffer);
}

int64_t timestamp_floor_hour(
    TExpressionContext* context,
    int64_t timestamp)
{
    (void)context;

    validate_timestamp(timestamp);

    time_t rawtime = timestamp;
    struct tm timeinfo;
    gmtime_r(&rawtime, &timeinfo);

    rawtime -= timeinfo.tm_min * 60;
    rawtime -= timeinfo.tm_sec;

    return rawtime;
}

int64_t timestamp_floor_day(
    TExpressionContext* context,
    int64_t timestamp)
{
    (void)context;

    validate_timestamp(timestamp);

    time_t rawtime = timestamp;
    struct tm timeinfo;
    gmtime_r(&rawtime, &timeinfo);

    rawtime -= timeinfo.tm_hour * 60 * 60;
    rawtime -= timeinfo.tm_min * 60;
    rawtime -= timeinfo.tm_sec;

    return rawtime;
}

int64_t timestamp_floor_week(
    TExpressionContext* context,
    int64_t timestamp)
{
    (void)context;

    validate_timestamp(timestamp);

    time_t rawtime = timestamp;
    struct tm timeinfo;
    gmtime_r(&rawtime, &timeinfo);

    int days_from_monday = (timeinfo.tm_wday + 6) % 7;

    rawtime -= days_from_monday * 24 * 60 * 60;
    rawtime -= timeinfo.tm_hour * 60 * 60;
    rawtime -= timeinfo.tm_min * 60;
    rawtime -= timeinfo.tm_sec;

    return rawtime;
}

int64_t timestamp_floor_month(
    TExpressionContext* context,
    int64_t timestamp)
{
    (void)context;

    validate_timestamp(timestamp);

    time_t rawtime = timestamp;
    struct tm timeinfo;
    gmtime_r(&rawtime, &timeinfo);

    rawtime -= (timeinfo.tm_mday - 1) * 24 * 60 * 60;
    rawtime -= timeinfo.tm_hour * 60 * 60;
    rawtime -= timeinfo.tm_min * 60;
    rawtime -= timeinfo.tm_sec;

    return rawtime;
}

int64_t timestamp_floor_year(
    TExpressionContext* context,
    int64_t timestamp)
{
    (void)context;

    validate_timestamp(timestamp);

    time_t rawtime = timestamp;
    struct tm timeinfo;
    gmtime_r(&rawtime, &timeinfo);

    rawtime -= timeinfo.tm_yday * 24 * 60 * 60;
    rawtime -= timeinfo.tm_hour * 60 * 60;
    rawtime -= timeinfo.tm_min * 60;
    rawtime -= timeinfo.tm_sec;

    return rawtime;
}

