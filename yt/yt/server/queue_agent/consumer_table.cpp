#include "consumer_table.h"

#include "dynamic_state.h"
#include "helpers.h"

#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/client.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NQueueAgent {

using namespace NTableClient;
using namespace NHiveClient;
using namespace NYPath;
using namespace NApi;
using namespace NConcurrency;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static TTableSchemaPtr BigRTConsumerTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("ShardId", EValueType::Uint64, ESortOrder::Ascending),
    TColumnSchema("Offset", EValueType::Uint64),
});

class TBigRTConsumerTable
    : public IConsumerTable
{
public:
    TBigRTConsumerTable(
        const IClientPtr& client,
        const TYPath& path)
        : Client_(client)
        , Path_(path)
    { }

    TFuture<std::vector<TPartitionInfo>> CollectPartitions(int expectedPartitionCount, bool withLastConsumeTime) const override
    {
        return BIND(&TBigRTConsumerTable::DoCollectPartitions, MakeStrong(this), expectedPartitionCount, withLastConsumeTime)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    static TError CheckSchema(const TTableSchema& schema)
    {
        if (auto [compatibility, error] = CheckTableSchemaCompatibility(
                schema,
                *BigRTConsumerTableSchema,
                /*ignoreSortOrder*/ true);
            compatibility != ESchemaCompatibility::FullyCompatible)
        {
            return TError("Consumer table is not recognized as a BigRT consumer table")
                << error;
        }

        return TError();
    }

private:
    IClientPtr Client_;
    TYPath Path_;

    std::vector<TPartitionInfo> DoCollectPartitions(int expectedPartitionCount, bool withLastConsumeTime) const
    {
        std::vector<TPartitionInfo> result;

        auto selectRowsResult = WaitFor(Client_->SelectRows(
            Format("[ShardId], [Offset] from [%v]", Path_)))
            .ValueOrThrow();

        // Note that after table construction table schema may have changed.
        // We must be prepared for that.

        CheckSchema(selectRowsResult.Rowset->GetSchema())
            .ThrowOnError();

        std::vector<ui64> shardIndices;
        for (const auto& row : selectRowsResult.Rowset->GetRows()) {
            YT_VERIFY(row.GetCount() == 2);

            const auto& shardIdValue = row[0];
            YT_VERIFY(shardIdValue.Type == EValueType::Uint64);

            if (shardIdValue.Data.Uint64 >= static_cast<ui32>(expectedPartitionCount)) {
                // This row does not correspond to any partition considering the expected partition count,
                // so just skip it.
                continue;
            }

            shardIndices.push_back(shardIdValue.Data.Uint64);

            const auto& offsetValue = row[1];
            YT_VERIFY(offsetValue.Type == EValueType::Uint64);

            // NB: in BigRT offsets encode the last read row, while we operate with the first unread row.
            result.emplace_back(TPartitionInfo{
                .PartitionIndex = static_cast<i64>(shardIdValue.Data.Uint64),
                .NextRowIndex = static_cast<i64>(offsetValue.Data.Uint64) + 1,
            });
        }

        if (!withLastConsumeTime) {
            return result;
        }

        // Now do versioned lookups in order to obtain timestamps.

        TUnversionedRowsBuilder builder;
        for (ui64 shardIndex : shardIndices) {
            builder.AddRow(shardIndex);
        }

        TVersionedLookupRowsOptions options;
        // This allows easier detection of key set change during the query.
        options.KeepMissingRows = true;

        auto versionedRowset = WaitFor(Client_->VersionedLookupRows(
            Path_,
            TNameTable::FromKeyColumns(BigRTConsumerTableSchema->GetKeyColumns()),
            builder.Build(),
            options))
            .ValueOrThrow();

        YT_VERIFY(versionedRowset->GetRows().size() == shardIndices.size());

        for (const auto& [index, versionedRow] : Enumerate(versionedRowset->GetRows())) {
            if (versionedRow.GetWriteTimestampCount() < 1) {
                THROW_ERROR_EXCEPTION("Shard set changed during collection");
            }
            auto timestamp = versionedRow.BeginWriteTimestamps()[0];
            result[index].LastConsumeTime = TimestampToInstant(timestamp).first;
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

IConsumerTablePtr CreateConsumerTable(
    const IClientPtr& client,
    const TYPath& path,
    const TTableSchema& schema)
{
    TBigRTConsumerTable::CheckSchema(schema)
        .ThrowOnError();

    return New<TBigRTConsumerTable>(client, path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
