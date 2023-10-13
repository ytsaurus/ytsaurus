#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TTabletAction final
{
public:
    DEFINE_BYVAL_RO_PROPERTY(ETabletActionKind, Kind);
    DEFINE_BYVAL_RO_PROPERTY(TTabletActionId, Id);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TTabletId>, TabletIds);
    DEFINE_BYVAL_RO_PROPERTY(TGuid, CorrelationId);

    DEFINE_BYREF_RO_PROPERTY(std::vector<TTabletCellId>, CellIds);
    DEFINE_BYREF_RO_PROPERTY(int, TabletCount);

    DEFINE_BYREF_RW_PROPERTY(TError, Error);
    DEFINE_BYVAL_RW_PROPERTY(ETabletActionState, State, ETabletActionState::Preparing);
    DEFINE_BYVAL_RW_PROPERTY(bool, Lost, false);

public:
    TTabletAction(
        TTabletActionId id,
        const TActionDescriptor& descriptor);

    bool IsFinished() const;
};

DEFINE_REFCOUNTED_TYPE(TTabletAction)

////////////////////////////////////////////////////////////////////////////////

bool IsTabletActionFinished(ETabletActionState state);

void FormatValue(TStringBuilderBase* builder, const TActionDescriptor& descriptor, TStringBuf /*format*/);
TString ToString(const TActionDescriptor& descriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
