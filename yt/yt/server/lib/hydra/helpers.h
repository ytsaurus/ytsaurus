#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <library/cpp/yt/error/error.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

bool IsPersistenceEnabled(
    const NElection::TCellManagerPtr& cellManager,
    const TDistributedHydraManagerOptions& options);

std::optional<TSharedRef> SanitizeLocalHostName(
    const THashSet<std::string>& clusterPeersAddresses,
    const std::string& host);

////////////////////////////////////////////////////////////////////////////////

template <typename TFunc, typename... TArgs>
auto InvokeAndWrapHydraException(TFunc&& func, TArgs&&... args);

TError WrapHydraError(TError&& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
