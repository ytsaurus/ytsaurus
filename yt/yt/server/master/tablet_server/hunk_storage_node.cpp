#include "hunk_storage_node.h"

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NCypressClient;
using namespace NTableServer;

////////////////////////////////////////////////////////////////////////////////

std::string THunkStorageNode::GetLowercaseObjectName() const
{
    return Format("hunk storage %v", GetId());
}

std::string THunkStorageNode::GetCapitalizedObjectName() const
{
    return Format("Hunk storage %v", GetId());
}

void THunkStorageNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TBase>();

    PHOENIX_REGISTER_FIELD(1, ReadQuorum_);
    PHOENIX_REGISTER_FIELD(2, WriteQuorum_);
    PHOENIX_REGISTER_FIELD(3, AssociatedNodeIds_);
}

PHOENIX_DEFINE_TYPE(THunkStorageNode);

void THunkStorageNode::ValidateRemount() const
{
    THROW_ERROR_EXCEPTION("Hunk storage does not support remount");
}

void THunkStorageNode::ValidateFreeze() const
{
    THROW_ERROR_EXCEPTION("Hunk storage does not support freeze");
}

void THunkStorageNode::ValidateUnfreeze() const
{
    THROW_ERROR_EXCEPTION("Hunk storage does not support unfreeze");
}

void THunkStorageNode::ValidateReshard(
    const TBootstrap* bootstrap,
    int firstTabletIndex,
    int lastTabletIndex,
    int newTabletCount,
    const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
    const std::vector<i64>& trimmedRowCounts,
    const std::vector<i64>& cumulativeDataWeights) const
{
    TTabletOwnerBase::ValidateReshard(
        bootstrap,
        firstTabletIndex,
        lastTabletIndex,
        newTabletCount,
        pivotKeys,
        trimmedRowCounts,
        cumulativeDataWeights);

    if (!trimmedRowCounts.empty()) {
        THROW_ERROR_EXCEPTION("Cannot reshard hunk storage node with \"trimmed_row_counts\"");
    }

    if (!cumulativeDataWeights.empty()) {
        THROW_ERROR_EXCEPTION("Cannot reshard hunk storage node with \"cumulative_data_weights\"");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
