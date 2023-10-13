#include "tablet_action.h"

#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

TTabletAction::TTabletAction(
    TTabletActionId id,
    const TActionDescriptor& descriptor)
    : Id_(id)
{
    Visit(descriptor,
        [&] (const TMoveDescriptor& descriptor) {
            Kind_ = ETabletActionKind::Move;
            TabletIds_.emplace_back(descriptor.TabletId);
            CellIds_.emplace_back(descriptor.TabletCellId);
            CorrelationId_ = descriptor.CorrelationId;
        },
        [&] (const TReshardDescriptor& descriptor) {
            Kind_ = ETabletActionKind::Reshard;
            TabletIds_ = std::move(descriptor.Tablets);
            TabletCount_ = descriptor.TabletCount;
            CorrelationId_ = descriptor.CorrelationId;
        });
}

bool TTabletAction::IsFinished() const
{
    return Lost_ || IsTabletActionFinished(State_);
}

////////////////////////////////////////////////////////////////////////////////

bool IsTabletActionFinished(ETabletActionState state)
{
    return state == ETabletActionState::Completed ||
        state == ETabletActionState::Failed;
}

void FormatValue(TStringBuilderBase* builder, const TActionDescriptor& descriptor, TStringBuf /*format*/)
{
    Visit(descriptor,
        [&] (const TMoveDescriptor& descriptor) {
            builder->AppendFormat(
                "MoveAction{TabletId: %v, CellId: %v}",
                descriptor.TabletId,
                descriptor.TabletCellId);
        },
        [&] (const TReshardDescriptor& descriptor) {
            builder->AppendFormat(
                "ReshardAction{TabletIds: %v, TabletCount: %v, DataSize: %v}",
                descriptor.Tablets,
                descriptor.TabletCount,
                descriptor.DataSize);
        });
}

TString ToString(const TActionDescriptor& descriptor)
{
    return ToStringViaBuilder(descriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
