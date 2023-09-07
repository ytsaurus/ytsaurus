#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/datetime/base.h>
#include <util/generic/fwd.h>

class TLog;

namespace NCachesPrivate {
    class TSimpleStats {
    public:
        TSimpleStats(const char* comment, int freq = 1000)
            : Comment(comment)
            , Freq(freq)
        {
        }

        void UpdateStats(TDuration duration, TStringBuf outlierComment, TLog& log);
        void PrintAndReset(TLog& log);
        void PrintSummary(TLog& log);

    private:
        TAtomic Cnt = 0;
        TAtomic TotalCnt = 0;
        TAtomic Time = 0;
        TAtomic TotalTime = 0;
        const char* Comment = "";
        int Freq = 1000;
        // Time considered outlier
        int Outlier = 100;
    };
}
