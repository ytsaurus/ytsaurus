#pragma once

#include "private.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <util/generic/map.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TStateHashChecker
    : public TRefCounted
{
public:
    TStateHashChecker(
        int limit,
        NLogging::TLogger logger);

    void Report(i64 sequenceNumber, ui64 stateHash, int peerId);
    THashMap<i64, ui64> GetStateHashes(const std::vector<i64>& sequenceNumbers);

    void ReconfigureLimit(int limit);

private:
    const NLogging::TLogger Logger;

    int Limit_;

    struct TReportedStateHash
    {
        int PeerId;
        ui64 StateHash;
    };
    std::map<i64, TReportedStateHash> SequenceNumberToStateHash_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
};

DEFINE_REFCOUNTED_TYPE(TStateHashChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
