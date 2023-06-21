#include "chaos_helpers.h"

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/server/master/table_server/table_node.h>

namespace NYT::NTabletServer {

using namespace NChaosClient;
using namespace NTableClient;
using namespace NTableServer;

////////////////////////////////////////////////////////////////////////////////

TReplicationProgress GatherReplicationProgress(const TTableNode* table)
{
    if (table->IsExternal()) {
        return {};
    }

    std::vector<TReplicationProgress> progresses;
    std::vector<TLegacyKey> pivotKeys;
    std::vector<TLegacyOwningKey> buffer;

    progresses.reserve(std::ssize(table->Tablets()));
    pivotKeys.reserve(std::ssize(table->Tablets()));

    for (int index = 0; index < std::ssize(table->Tablets()); ++index) {
        auto* tablet = table->Tablets()[index]->As<TTablet>();
        progresses.push_back(tablet->ReplicationProgress());
        pivotKeys.push_back(GetTabletReplicationProgressPivotKey(tablet, index, &buffer));
    }

    return NChaosClient::GatherReplicationProgress(std::move(progresses), pivotKeys, MaxKey().Get());
}

void ScatterReplicationProgress(NTableServer::TTableNode* table, TReplicationProgress progress)
{
    if (table->IsExternal()) {
        return;
    }

    std::vector<TLegacyKey> pivotKeys;
    std::vector<TLegacyOwningKey> buffer;
    pivotKeys.reserve(std::ssize(table->Tablets()));

    for (int index = 0; index < std::ssize(table->Tablets()); ++index) {
        auto* tablet = table->Tablets()[index]->As<TTablet>();
        pivotKeys.push_back(GetTabletReplicationProgressPivotKey(tablet, index, &buffer));
    }

    auto newProgresses = NChaosClient::ScatterReplicationProgress(
        std::move(progress),
        pivotKeys,
        MaxKey().Get());

    for (int index = 0; index < std::ssize(table->Tablets()); ++index) {
        auto* tablet = table->Tablets()[index]->As<TTablet>();
        tablet->ReplicationProgress() = std::move(newProgresses[index]);
    }
}

TLegacyKey GetTabletReplicationProgressPivotKey(
    TTablet* tablet,
    int tabletIndex,
    std::vector<TLegacyOwningKey>* buffer)
{
    if (tablet->GetTable()->IsSorted()) {
        return tablet->GetPivotKey().Get();
    } else if (tabletIndex == 0) {
        return EmptyKey().Get();
    } else {
        buffer->push_back(MakeUnversionedOwningRow(tabletIndex));
        return buffer->back().Get();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
