#pragma once

#include "private.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

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

    void Report(i64 sequenceNumber, ui64 stateHash);
    std::optional<ui64> GetStateHash(i64 sequenceNumber);


private:
    const int Limit_;
    const NLogging::TLogger Logger;

    std::map<i64, ui64> SequenceNumberToStateHash_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
};

DEFINE_REFCOUNTED_TYPE(TStateHashChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
