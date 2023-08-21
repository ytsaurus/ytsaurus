#include "helpers.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/queue_client/dynamic_state.h>

#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NQueueAgent {

using namespace NObjectClient;
using namespace NConcurrency;
using namespace NYPath;
using namespace NLogging;
using namespace NTableClient;
using namespace NQueueClient;

////////////////////////////////////////////////////////////////////////////////

TErrorOr<EQueueFamily> DeduceQueueFamily(const TQueueTableRow& row)
{
    if (!row.RowRevision) {
        return TError("Queue is not in-sync yet");
    }
    if (!row.ObjectType) {
        return TError("Object type is not known yet");
    }

    if (row.ObjectType == EObjectType::Table) {
        // NB: Dynamic and Sorted or optionals.
        if (row.Dynamic == true && row.Sorted == false) {
            return EQueueFamily::OrderedDynamicTable;
        }
        return TError("Only ordered dynamic tables are supported as queues");
    }

    return TError("Invalid queue object type %Qlv", row.ObjectType);
}

bool IsReplicatedTableObjectType(EObjectType type)
{
    return type == EObjectType::ReplicatedTable || type == EObjectType::ChaosReplicatedTable;
}

bool IsReplicatedTableObjectType(const std::optional<EObjectType>& type)
{
    return type && IsReplicatedTableObjectType(*type);
}

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IClientPtr AssertNativeClient(const NApi::IClientPtr& client)
{
    auto nativeClient = MakeStrong(dynamic_cast<NApi::NNative::IClient*>(client.Get()));
    YT_VERIFY(nativeClient);
    return nativeClient;
}

////////////////////////////////////////////////////////////////////////////////

//! Collect cumulative row indices from rows with given (tablet_index, row_index) pairs and
//! return them as a tablet_index: (row_index: cumulative_data_weight) map
THashMap<int, THashMap<i64, i64>> CollectCumulativeDataWeights(
    const TYPath& path,
    NApi::IClientPtr client,
    const std::vector<std::pair<int, i64>>& tabletAndRowIndices,
    const TLogger& logger)
{
    const auto& Logger = logger;

    if (tabletAndRowIndices.empty()) {
        return {};
    }

    TStringBuilder queryBuilder;
    queryBuilder.AppendFormat("[$tablet_index], [$row_index], [$cumulative_data_weight] from [%v] where ([$tablet_index], [$row_index]) in (",
        path);
    bool isFirstTuple = true;
    for (const auto& [partitionIndex, rowIndex] : tabletAndRowIndices) {
        if (!isFirstTuple) {
            queryBuilder.AppendString(", ");
        }
        queryBuilder.AppendFormat("(%vu, %vu)", partitionIndex, rowIndex);
        isFirstTuple = false;
    }

    queryBuilder.AppendString(")");

    YT_VERIFY(!isFirstTuple);

    auto query = queryBuilder.Flush();
    YT_LOG_TRACE("Executing query for cumulative data weights (Query: %v)", query);
    auto selectResult = WaitFor(client->SelectRows(query))
        .ValueOrThrow();

    THashMap<int, THashMap<i64, i64>> result;

    for (const auto& row : selectResult.Rowset->GetRows()) {
        YT_VERIFY(row.GetCount() == 3);

        auto tabletIndex = FromUnversionedValue<int>(row[0]);
        auto rowIndex = FromUnversionedValue<i64>(row[1]);
        auto cumulativeDataWeight = FromUnversionedValue<std::optional<i64>>(row[2]);

        if (!cumulativeDataWeight) {
            continue;
        }

        result[tabletIndex].emplace(rowIndex, *cumulativeDataWeight);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> OptionalSub(const std::optional<i64> lhs, const std::optional<i64> rhs)
{
    if (lhs && rhs) {
        return *lhs - *rhs;
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
