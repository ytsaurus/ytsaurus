#include "object.h"

#include "snapshot.h"

namespace NYT::NQueueAgent {

template <typename TSnapshot>
TErrorController<TSnapshot>::TErrorController(
    TSnapshotPtr snapshot)
    : Snapshot_(std::move(snapshot))
{ }

template <typename TSnapshot>
void TErrorController<TSnapshot>::OnDynamicConfigChanged(
    const TQueueControllerDynamicConfigPtr& /*oldConfig*/,
    const TQueueControllerDynamicConfigPtr& /*newConfig*/)
{ }

template <typename TSnapshot>
void TErrorController<TSnapshot>::OnRowUpdated(std::any /*row*/)
{
    // Row update is handled in Update Controller.
}

template <typename TSnapshot>
void TErrorController<TSnapshot>::OnReplicatedTableMappingRowUpdated(const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& /*row*/)
{
    // Row update is handled in Update Controller.
}

template <typename TSnapshot>
void TErrorController<TSnapshot>::Stop()
{ }

template <typename TSnapshot>
TRefCountedPtr TErrorController<TSnapshot>::GetLatestSnapshot() const
{
    return Snapshot_;
}

template <typename TSnapshot>
void TErrorController<TSnapshot>::BuildOrchid(NYson::IYsonConsumer* consumer) const
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("row").Value(Snapshot_->Row)
            .Item("replicated_table_mapping_row").Value(Snapshot_->ReplicatedTableMappingRow)
            .Item("status").BeginMap()
                .Item("error").Value(Snapshot_->Error)
            .EndMap()
            .Item("partitions").BeginList().EndList()
        .EndMap();
}

template <typename TSnapshot>
bool TErrorController<TSnapshot>::IsLeading() const
{
    return false;
}

template class TErrorController<TQueueSnapshot>;

} // namespace NYT::NQueueAgent
