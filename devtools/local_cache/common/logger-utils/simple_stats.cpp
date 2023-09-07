#include "simple_stats.h"
#include <library/cpp/logger/global/rty_formater.h>

void NCachesPrivate::TSimpleStats::UpdateStats(TDuration duration, TStringBuf outlierComment, TLog& log) {
    auto cnt = AtomicAdd(Cnt, 1);
    AtomicAdd(TotalCnt, 1);
    AtomicAdd(Time, static_cast<TAtomic>(duration.MicroSeconds()));
    AtomicAdd(TotalTime, static_cast<TAtomic>(duration.MicroSeconds()));
    if (duration > TDuration::MilliSeconds(Outlier)) {
        LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_INFO, "INFO[STAT]") << "Outlier " << Comment << " time: "
                                                                                      << (double)duration.MicroSeconds() / 1000 << "msec ("
                                                                                      << outlierComment << ")" << Endl;
    }
    if (cnt == Freq) {
        PrintAndReset(log);
    }
}

void NCachesPrivate::TSimpleStats::PrintAndReset(TLog& log) {
    // Allowed to drop some data during races.
    auto cnt = AtomicSwap(&Cnt, 0);
    auto total = AtomicSwap(&Time, 0);
    if (cnt > 0) {
        LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_INFO, "INFO[STAT]") << "Ave " << Comment << " time: "
                                                                                      << (double)total / (1000 * cnt) << "msec" << Endl;
    }
}

void NCachesPrivate::TSimpleStats::PrintSummary(TLog& log) {
    // Allowed to drop some data during races.
    auto cnt = AtomicSwap(&TotalCnt, 0);
    auto total = AtomicSwap(&TotalTime, 0);
    if (cnt > 0) {
        LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_INFO, "INFO[STAT]") << "Final ave " << Comment << " time: "
                                                                                      << (double)total / (1000 * cnt) << "msec, "
                                                                                      << "total time: " << (double)total / 1000 << "msec" << Endl;
    }
}
