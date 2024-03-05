#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer::NDryRun {

////////////////////////////////////////////////////////////////////////////////

struct TTabletActionBatch
{
    std::vector<TMoveDescriptor> MoveDescriptors;
    std::vector<TReshardDescriptor> ReshardDescriptors;
};

void PrintDescriptors(const std::vector<TMoveDescriptor>& descriptors);
void PrintDescriptors(const std::vector<TReshardDescriptor>& descriptors);
void PrintDescriptors(const TTabletActionBatch& descriptors);

void ApplyMoveDescriptors(
    const TTabletCellBundlePtr& bundle,
    const std::vector<TMoveDescriptor>& descriptors);

TTabletActionBatch Balance(
    EBalancingMode mode,
    const TTabletCellBundlePtr& bundle,
    const TString& group,
    const TString& parameterizedConfig);

TTabletActionBatch BalanceAndPrintDescriptors(
    EBalancingMode mode,
    const TTabletCellBundlePtr& bundle,
    const TString& group,
    const TString& parameterizedConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer::NDryRun
