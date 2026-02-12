#include "object.h"

#include "snapshot.h"

namespace NYT::NQueueAgent {

template <typename TRow, typename TSnapshot>
TErrorController<TRow, TSnapshot>::TErrorController(
    TRow row,
    std::optional<NQueueClient::TReplicatedTableMappingTableRow> replicatedTableMappingRow,
    TError error)
    : Row_(std::move(row))
    , ReplicatedTableMappingRow_(std::move(replicatedTableMappingRow))
    , Error_(std::move(error))
    , Snapshot_(New<TSnapshot>())
{
    Snapshot_->Error = Error_;
}

template <typename TRow, typename TSnapshot>
void TErrorController<TRow, TSnapshot>::OnDynamicConfigChanged(
    const TQueueControllerDynamicConfigPtr& /*oldConfig*/,
    const TQueueControllerDynamicConfigPtr& /*newConfig*/)
{ }

template <typename TRow, typename TSnapshot>
void TErrorController<TRow, TSnapshot>::OnRowUpdated(std::any /*row*/)
{
    // Row update is handled in Update Controller.
}

template <typename TRow, typename TSnapshot>
void TErrorController<TRow, TSnapshot>::OnReplicatedTableMappingRowUpdated(const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& /*row*/)
{
    // Row update is handled in Update Controller.
}

template <typename TRow, typename TSnapshot>
void TErrorController<TRow, TSnapshot>::Stop()
{ }

template <typename TRow, typename TSnapshot>
TRefCountedPtr TErrorController<TRow, TSnapshot>::GetLatestSnapshot() const
{
    return Snapshot_;
}

template <typename TRow, typename TSnapshot>
void TErrorController<TRow, TSnapshot>::BuildOrchid(NYson::IYsonConsumer* consumer) const
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("row").Value(Row_)
            .Item("replicated_table_mapping_row").Value(ReplicatedTableMappingRow_)
            .Item("status").BeginMap()
                .Item("error").Value(Error_)
            .EndMap()
            .Item("partitions").BeginList().EndList()
        .EndMap();
}

template <typename TRow, typename TSnapshot>
bool TErrorController<TRow, TSnapshot>::IsLeading() const
{
    return false;
}

template class TErrorController<NQueueClient::TConsumerTableRow, TConsumerSnapshot>;
template class TErrorController<NQueueClient::TQueueTableRow, TQueueSnapshot>;

} // namespace NYT::NQueueAgent
