#include "yt_udf.h"

#include <time.h>

int64_t sleep(
    TExpressionContext* context,
    int64_t value)
{
    (void)context;
    if (value < 1) {
        value = 1;
    }
    if (value > 1000) {
        value = 1000;
    }
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = value * 1000000;
    nanosleep(&ts, NULL);
    return 0;
}
