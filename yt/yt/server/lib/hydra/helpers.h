#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

bool IsPersistenceEnabled(
    const NElection::TCellManagerPtr& cellManager,
    const TDistributedHydraManagerOptions& options);

std::optional<TSharedRef> SanitizeLocalHostName(
    const THashSet<TString>& clusterPeersAddresses,
    TStringBuf host);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
