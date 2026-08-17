#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/error/error.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

bool IsPersistenceEnabled(
    const NElection::TCellManagerPtr& cellManager,
    const TDistributedHydraManagerOptions& options);

std::optional<TSharedRef> SanitizeLocalHostName(
    const THashSet<std::string>& clusterPeersAddresses,
    const std::string& host);

std::vector<i64> SampleMutationsSequenceNumbers(
    i64 firstSequenceNumber,
    i64 lastSequenceNumber,
    i64 rate);

TFuture<void> ReportMutationStateHashesToLeader(
    const NElection::TCellManagerPtr& cellManager,
    int leaderId,
    const std::vector<std::pair<i64, ui64>>& sequenceNumbersToStateHashes,
    TDuration timeout,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

template <typename TFunc, typename... TArgs>
auto InvokeAndWrapHydraException(TFunc&& func, TArgs&&... args);

TError WrapHydraError(TError&& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
