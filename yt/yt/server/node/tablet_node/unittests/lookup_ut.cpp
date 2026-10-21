#include "tablet_context_mock.h"
#include "sorted_store_manager_ut_helpers.h"

#include <yt/yt/server/node/tablet_node/cached_row.h>
#include <yt/yt/server/node/tablet_node/store_manager_detail.h>
#include <yt/yt/server/node/tablet_node/structured_logger.h>
#include <yt/yt/server/node/tablet_node/tablet_reader.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/versioned_block_writer.h>

#include <yt/yt/library/row_merger/row_merger.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_reader.h>

#include <yt/yt/client/transaction_client/ts_literal.h>

#include <yt/yt/core/misc/slab_allocator.h>

#include <yt/yt/core/test_framework/framework.h>

#include <algorithm>
#include <array>

namespace NYT::NTabletNode {
namespace {

using namespace NCypressClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NObjectClient;

using namespace NTableClient::NProto;

using NTransactionClient::operator""_ts;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

const NTableClient::TTableSchemaPtr Schema1 = New<TTableSchema>(std::vector{
    TColumnSchema(TColumnSchema("k", EValueType::Int64).SetSortOrder(NTableClient::ESortOrder::Ascending)),
    TColumnSchema(TColumnSchema("v", EValueType::Int64)),
});

const NTableClient::TTableSchemaPtr Schema2 = New<TTableSchema>(std::vector{
    TColumnSchema(TColumnSchema("k", EValueType::Int64).SetSortOrder(NTableClient::ESortOrder::Ascending)),
    TColumnSchema(TColumnSchema("k2", EValueType::Int64).SetSortOrder(NTableClient::ESortOrder::Ascending)),
    TColumnSchema(TColumnSchema("v2", EValueType::Int64)),
    TColumnSchema(TColumnSchema("v", EValueType::Int64)),
});

////////////////////////////////////////////////////////////////////////////////

class TLookupTest
    : public TSortedStoreManagerTestBase
{
public:
    void SetUp() override
    {
        // NB: Tablet setup is postponed until stores creation.
        TableSchema_ = Schema1;
    }

    TTableSchemaPtr GetSchema() const override
    {
        YT_VERIFY(TableSchema_);
        return TableSchema_;
    }

    void SetTableSchema(const TTableSchemaPtr& schema)
    {
        TableSchema_ = schema;
    }

    void AddChunkStore(
        bool eden,
        const std::vector<TVersionedOwningRow>& rows,
        const TTableSchemaPtr& chunkSchema = nullptr,
        TFuture<void> metaReadAllowedToProceedFuture = OKFuture,
        TPromise<void> notifyMetaReadStartedPromise = {})
    {
        NTabletNode::NProto::TAddStoreDescriptor descriptor;
        descriptor.set_store_type(ToProto(EStoreType::SortedChunk));
        auto storeId = MakeId(
            EObjectType::Chunk,
            InvalidCellTag,
            AddStoreDescriptors_.size(),
            /*hash*/ 0);
        ToProto(descriptor.mutable_store_id(), storeId);

        auto chunkData = ConstructChunkData(
            rows,
            chunkSchema ? chunkSchema : TableSchema_);

        ToProto(descriptor.mutable_chunk_meta(), static_cast<NChunkClient::NProto::TChunkMeta>(*chunkData.Meta));

        OnNewChunkStore(std::move(descriptor), eden);

        TabletContext_.GetBackendChunkReadersHolder()->RegisterBackendChunkReader(
            storeId,
            chunkData,
            std::move(metaReadAllowedToProceedFuture),
            std::move(notifyMetaReadStartedPromise));
    }

    void OnChunkStoresAdded()
    {
        TSortedStoreManagerTestBase::SetUp();
    }

    TUnversionedOwningRow BuildRow(TStringBuf rowString)
    {
        return YsonToSchemafulRow(rowString, *TableSchema_, /*treatMissingAsNull*/ false);
    }

    TFuture<std::vector<TUnversionedOwningRow>> StartDoLookupRows(
        const std::vector<TUnversionedRow>& keys,
        TTimestamp timestamp = SyncLastCommittedTimestamp,
        std::optional<TTimestamp> retentionTimestamp = std::nullopt,
        const std::vector<int>& columnIndexes = {},
        TTabletSnapshotPtr tabletSnapshot = nullptr,
        NChunkClient::TClientChunkReadOptions chunkReadOptions = TClientChunkReadOptions())
    {
        TReadTimestampRange timestampRange {
            .Timestamp = timestamp,
        };
        if (retentionTimestamp) {
            timestampRange.RetentionTimestamp = *retentionTimestamp;
        }

        chunkReadOptions.InitialQueryKind = EInitialQueryKind::LookupRows;

        return BIND(&LookupRowsImpl,
            Tablet_.get(),
            keys,
            timestampRange,
            columnIndexes,
            tabletSnapshot,
            chunkReadOptions)
            .AsyncVia(LookupQueue_->GetInvoker())
            .Run();
    }

    std::vector<TUnversionedOwningRow> DoLookupRows(
        const std::vector<TUnversionedRow>& keys,
        TTimestamp timestamp = SyncLastCommittedTimestamp,
        std::optional<TTimestamp> retentionTimestamp = std::nullopt,
        const std::vector<int>& columnIndexes = {},
        TTabletSnapshotPtr tabletSnapshot = nullptr,
        NChunkClient::TClientChunkReadOptions chunkReadOptions = TClientChunkReadOptions())
    {
        return WaitFor(StartDoLookupRows(
            keys,
            timestamp,
            retentionTimestamp,
            columnIndexes,
            tabletSnapshot,
            chunkReadOptions))
            .ValueOrThrow();
    }

    TUnversionedOwningRow DoLockProjection(
        TStringBuf readerKind,
        const TUnversionedOwningRow& key,
        TTimestamp timestamp,
        TTimestamp retentionTimestamp,
        const std::vector<int>& columns)
    {
        if (readerKind == "Lookup") {
            return DoLookupRows({key}, timestamp, retentionTimestamp, columns)[0];
        }

        return WaitFor(BIND([=, this] {
            auto snapshot = Tablet_->BuildSnapshot(/*slot*/ nullptr);
            auto filter = columns.empty() ? TColumnFilter() : TColumnFilter(columns);
            auto bounds = MakeSingletonRowRange(key, BuildKey(Format("%v", key[0].Data.Int64 + 1)));
            TReadTimestampRange timestamps{.Timestamp = timestamp, .RetentionTimestamp = retentionTimestamp};

            if (readerKind == "VersionedLookup" || readerKind == "VersionedRange") {
                // Normalize versioned results only for value comparison; the underlying
                // lookup/range readers still produce all versions and check all locks.
                NRowMerger::TSchemafulRowMerger merger(
                    New<TRowBuffer>(),
                    snapshot->PhysicalSchema->GetColumnCount(),
                    /*keyColumnCount*/ 1,
                    filter,
                    snapshot->ColumnEvaluator,
                    retentionTimestamp);

                if (readerKind == "VersionedLookup") {
                    merger.AddPartialRow(
                        VersionedLookupRowImpl(
                            Tablet_.get(),
                            key,
                            /*minDataVersions*/ 100,
                            timestamp,
                            ChunkReadOptions_),
                        /*upperTimestampLimit*/ MaxTimestamp);
                } else {
                    for (const auto& store : GetStoresAndBounds(snapshot, /*partitionIndex*/ 0, bounds).Stores) {
                        auto reader = store->CreateReader(
                            snapshot,
                            bounds,
                            timestamp,
                            /*produceAllVersions*/ true,
                            /*columnFilter*/ TColumnFilter(),
                            ChunkReadOptions_,
                            /*workloadCategory*/ std::nullopt);
                        WaitFor(reader->Open())
                            .ThrowOnError();

                        while (auto batch = reader->Read(TRowBatchReadOptions{})) {
                            for (const auto& row : batch->MaterializeRows()) {
                                merger.AddPartialRow(row, /*upperTimestampLimit*/ MaxTimestamp);
                            }

                            WaitFor(reader->GetReadyEvent())
                                .ThrowOnError();
                        }
                    }
                }

                return TUnversionedOwningRow(merger.BuildMergedRow());
            }

            YT_VERIFY(readerKind == "Range" || readerKind == "Scan");

            auto reader = readerKind == "Range"
                ? CreateSchemafulSortedTabletReader(
                    snapshot,
                    filter,
                    bounds,
                    timestamps,
                    ChunkReadOptions_,
                    /*tabletThrottlerKind*/ std::nullopt,
                    /*workloadCategory*/ std::nullopt,
                    /*timestampReadOptions*/ {})
                : DoCreateScanReader(
                    snapshot,
                    filter,
                    GetStoresAndBounds(snapshot, /*partitionIndex*/ 0, bounds),
                    timestamps,
                    ChunkReadOptions_,
                    /*tabletThrottlerKind*/ std::nullopt,
                    /*workloadCategory*/ std::nullopt,
                    /*timestampReadOptions*/ {},
                    /*mergeVersionedRows*/ true);

            TUnversionedOwningRow result;
            while (auto batch = reader->Read()) {
                for (const auto& row : batch->MaterializeRows()) {
                    EXPECT_FALSE(result);
                    result = TUnversionedOwningRow(row);
                }

                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
            }

            return result;
        })
            .AsyncVia(LookupQueue_->GetInvoker())
            .Run())
            .ValueOrThrow();
    }

    TVersionedOwningRow DoVersionedLookupRow(const TUnversionedOwningRow& key)
    {
        return WaitFor(BIND(&VersionedLookupRowImpl,
            Tablet_.get(),
            key,
            /*minDataVersions*/ 100,
            AsyncLastCommittedTimestamp,
            ChunkReadOptions_)
            .AsyncVia(LookupQueue_->GetInvoker())
            .Run())
            .ValueOrThrow();
    }

    void ValidateLookup(
        const std::vector<TUnversionedRow>& keys,
        const std::vector<TUnversionedOwningRow>& expected)
    {
        EXPECT_EQ(expected, DoLookupRows(keys));
    }

private:
    const TActionQueuePtr LookupQueue_ = New<TActionQueue>("LookupTest");

    TTableSchemaPtr TableSchema_;

    TChunkData ConstructChunkData(
        const std::vector<TVersionedOwningRow>& rows,
        const TTableSchemaPtr& chunkSchema)
    {
        YT_VERIFY(!rows.empty());

        NChunkClient::NProto::TChunkMeta chunkMeta;
        chunkMeta.set_type(ToProto(EChunkType::Table));
        chunkMeta.set_format(ToProto(EChunkFormat::TableVersionedSimple));

        TTimestamp minTimestamp;
        TTimestamp maxTimestamp;
        i64 dataWeight = 0;
        for (const auto& row : rows) {
            for (auto timestamp : row.WriteTimestamps()) {
                minTimestamp = std::min(minTimestamp, timestamp);
                maxTimestamp = std::max(maxTimestamp, timestamp);
            }
            for (auto timestamp : row.DeleteTimestamps()) {
                minTimestamp = std::min(minTimestamp, timestamp);
                maxTimestamp = std::max(maxTimestamp, timestamp);
            }
            dataWeight += GetDataWeight(row);
        }

        NChunkClient::NProto::TMiscExt miscExt;
        miscExt.set_min_timestamp(ToProto(minTimestamp));
        miscExt.set_max_timestamp(ToProto(maxTimestamp));
        miscExt.set_sorted(true);
        miscExt.set_row_count(rows.size());
        miscExt.set_data_weight(dataWeight);
        miscExt.set_compressed_data_size(dataWeight);
        miscExt.set_uncompressed_data_size(dataWeight);
        SetProtoExtension(chunkMeta.mutable_extensions(), miscExt);

        TBoundaryKeysExt boundaryKeysExt;
        ToProto(
            boundaryKeysExt.mutable_min(),
            TLegacyOwningKey(rows.begin()->Keys()));
        ToProto(
            boundaryKeysExt.mutable_max(),
            TLegacyOwningKey(rows.back().Keys()));
        SetProtoExtension(chunkMeta.mutable_extensions(), boundaryKeysExt);

        SetProtoExtension(chunkMeta.mutable_extensions(), ToProto<TTableSchemaExt>(chunkSchema));

        TDataBlockMetaExt dataBlockMetaExt;

        auto blockWriter = TSimpleVersionedBlockWriter(chunkSchema);
        for (const auto& row : rows) {
            blockWriter.WriteRow(row);
        }
        auto block = blockWriter.FlushBlock();

        block.Meta.set_chunk_row_count(rows.size());
        block.Meta.set_block_index(0);
        ToProto(block.Meta.mutable_last_key(), rows.back().Keys());

        dataBlockMetaExt.add_data_blocks()->Swap(&block.Meta);

        SetProtoExtension(chunkMeta.mutable_extensions(), dataBlockMetaExt);

        auto codec = NCompression::GetCodec(NCompression::ECodec::None);
        auto blockData = codec->Compress(block.Data);

        return TChunkData{
            .Blocks = {blockData},
            .Meta = New<TRefCountedChunkMeta>(chunkMeta)
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TLookupTest, Simple)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        });

    OnChunkStoresAdded();

    ValidateLookup(
        {YsonToKey("0")},
        {BuildRow("k=0;v=0")});
}

TEST_F(TLookupTest, Empty)
{
    OnChunkStoresAdded();

    EXPECT_FALSE(DoLookupRows({YsonToKey("0")})[0]);
}

TEST_F(TLookupTest, OverlappingChunks)
{
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=100> 1"),
            YsonToVersionedRow(
                "<id=0> 2",
                "<id=1;ts=100> 2"),
        });
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 2",
                "<id=1;ts=300> 3"),
            YsonToVersionedRow(
                "<id=0> 3",
                "<id=1;ts=100> 4"),
        });
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=200> 5"),
            YsonToVersionedRow(
                "<id=0> 2",
                "<id=1;ts=200> 6"),
            YsonToVersionedRow(
                "<id=0> 4",
                "<id=1;ts=100> 7"),
        });

    OnChunkStoresAdded();

    ValidateLookup(
        {YsonToKey("0"), YsonToKey("2"), YsonToKey("3")},
        {BuildRow("k=0;v=0"), BuildRow("k=2;v=3"), BuildRow("k=3;v=4")});
    ValidateLookup(
        {YsonToKey("1"), YsonToKey("4")},
        {BuildRow("k=1;v=5"), BuildRow("k=4;v=7")});
}

TEST_F(TLookupTest, RetentionTimestamp)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        });

    OnChunkStoresAdded();

    EXPECT_FALSE(
        DoLookupRows(
            {YsonToKey("0")},
            /*timestamp*/ SyncLastCommittedTimestamp,
            /*retentionTimestamp*/ 200_ts)[0]);
}

TEST_F(TLookupTest, ColumnFilter)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        });

    OnChunkStoresAdded();

    EXPECT_EQ(
        DoLookupRows(
            {YsonToKey("0")},
            SyncLastCommittedTimestamp,
            std::nullopt,
            {0}),
        std::vector<TUnversionedOwningRow>({BuildRow("k=0")}));
}

TEST_F(TLookupTest, MissingRow)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
            YsonToVersionedRow(
                "<id=0> 2",
                "<id=1;ts=100> 2"),
        });

    OnChunkStoresAdded();

    auto lookupResult = DoLookupRows({YsonToKey("0"), YsonToKey("1"), YsonToKey("2")});
    EXPECT_EQ(BuildRow("k=0;v=0"), lookupResult[0]);
    EXPECT_FALSE(lookupResult[1]);
    EXPECT_EQ(BuildRow("k=2;v=2"), lookupResult[2]);
}

TEST_F(TLookupTest, DeletedRow)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        });
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "",
                {200_ts}),
            YsonToVersionedRow(
                "<id=0> 1",
                "",
                {200_ts}),
        });

    OnChunkStoresAdded();

    EXPECT_FALSE(DoLookupRows({YsonToKey("0")})[0]);
    EXPECT_FALSE(DoLookupRows({YsonToKey("1")})[0]);
}

TEST_F(TLookupTest, ExplicitTimestamp)
{
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=100> 1",
                {200_ts}),
        });
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=200> 1"),
        });

    OnChunkStoresAdded();

    EXPECT_EQ(
        DoLookupRows(
            {YsonToKey("0"), YsonToKey("1")},
            150_ts),
        std::vector<TUnversionedOwningRow>({BuildRow("k=0;v=0"), BuildRow("k=1;v=1")}));
}

TEST_F(TLookupTest, SchemaAlter1)
{
    SetTableSchema(Schema2);

    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        },
        Schema1);

    OnChunkStoresAdded();

    ValidateLookup(
        {YsonToKey("0;#")},
        {BuildRow("k=0;k2=#;v=0;v2=#")});
}

TEST_F(TLookupTest, SchemaAlter2)
{
    SetTableSchema(Schema2);

    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        },
        Schema1);
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 0; <id=1> #",
                "<id=2;ts=200> 1"),
        });

    OnChunkStoresAdded();

    ValidateLookup(
        {YsonToKey("0;#")},
        {BuildRow("k=0;k2=#;v=0;v2=1")});
}

TEST_F(TLookupTest, DynamicStores)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=100> 1"),
            YsonToVersionedRow(
                "<id=0> 2",
                "<id=1;ts=100> 2"),
        });

    OnChunkStoresAdded();

    WriteRow(BuildRow("k=1;v=2"));
    WriteRow(BuildRow("k=2;v=3"));

    RotateStores();

    WriteRow(BuildRow("k=2;v=4"));

    ValidateLookup(
        {YsonToKey("0"), YsonToKey("1"), YsonToKey("2")},
        {BuildRow("k=0;v=0"), BuildRow("k=1;v=2"), BuildRow("k=2;v=4")});
}

class TLockVisibilityBoundaryTest
    : public TLookupTest
    , public ::testing::WithParamInterface<std::tuple<TStringBuf, TStringBuf, TStringBuf, ELockType>>
{
public:
    static constexpr TStringBuf Placements[] = {"Active", "HistoryPassive", "PreparedPassive"};
    static constexpr ELockType LockTypes[] = {
        ELockType::None,
        ELockType::SharedWeak,
        ELockType::SharedStrong,
        ELockType::Exclusive,
        ELockType::SharedWrite,
    };

    static std::vector<ParamType> GenerateMatrixCases(TStringBuf history)
    {
        std::vector<ParamType> cases;
        for (TStringBuf kind : {"Lock", "Write", "Add", "Delete"}) {
            THashSet<ELockType> firstLockTypes;
            for (const auto& lockTypes : GenerateLockSets(kind)) {
                if (!firstLockTypes.insert(lockTypes.front()).second) {
                    continue;
                }

                for (auto placement : Placements) {
                    cases.emplace_back(history, placement, kind, lockTypes.front());
                }
            }
        }

        return cases;
    }

    static std::string FormatCaseName(const ::testing::TestParamInfo<ParamType>& info)
    {
        const auto& [history, placement, kind, firstLockType] = info.param;
        return Format("%v_%v_%v", placement, kind, firstLockType);
    }

protected:
    struct TAction
    {
        TStringBuf Kind;
        std::string Transaction;
        std::string Row;
        std::vector<std::pair<TStringBuf, ELockType>> Locks;
    };

    using TSequence = std::vector<TAction>;
    using TWriterMask = ui32;

    struct TTransactionState
    {
        // Model state comes from the scenario, not from store descriptors.
        TAction Action;
        TUnversionedOwningRow Row;
        int Store = 0;
        TTimestamp Prepare = NullTimestamp;
        TTimestamp Commit = NullTimestamp;
        int CommitStore = 0;
        bool Finished = false;

        std::unique_ptr<TTestTransaction> Actual;
        TSortedDynamicRowRef RowRef;
    };

    struct TRead
    {
        TStringBuf Name;
        TTimestamp Timestamp;
        TTimestamp Retention = NullTimestamp;
    };

    struct TProjection
    {
        TStringBuf Name;
        std::vector<int> Columns;
    };

    static constexpr int ColumnCount = 5;
    static constexpr int KeyColumnId = 0;
    static constexpr int CounterColumnId = 2;
    static constexpr int ColumnGroups[] = {0, 1, 2, 3, 0};

    const std::vector<TProjection> Projections_ = {
        {"Keys", {KeyColumnId}},
        {"Payload", {KeyColumnId, 1}},
        {"Counter", {KeyColumnId, CounterColumnId}},
        {"Other", {KeyColumnId, 3}},
        {"Primary", {KeyColumnId, 4}},
        {"All", {}},
    };

    const THashMap<TStringBuf, int> LockGroups_ = {
        {"primary", 0},
        {"v", 1},
        {"counter", 2},
        {"other", 3},
        {"p", 0},
    };

    int CurrentStore_ = 0;
    THashMap<std::string, TTransactionState> Transactions_;
    std::vector<const TTransactionState*> History_;

    std::vector<std::pair<TLockDescriptor*, TTimestamp>> ProbeLocks_;

    void SetUp() override
    {
        TLookupTest::SetUp();

        SetTableSchema(New<TTableSchema>(std::vector{
            TColumnSchema("k", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("v", EValueType::Int64).SetLock("v"),
            TColumnSchema("counter", EValueType::Int64).SetLock("counter").SetAggregate("sum"),
            TColumnSchema("other", EValueType::Int64).SetLock("other"),
            TColumnSchema("p", EValueType::Int64),
        }));

        OnChunkStoresAdded();
    }

    void InstallProbe()
    {
        GetActiveStore()->SetRowBlockedHandler(BIND([this] (
            TSortedDynamicRow row,
            TSortedDynamicStore::TConflictInfo conflict,
            TDuration /*timeout*/)
        {
            // Test-only hack: register the wait and complete it immediately by
            // temporarily hiding the blocking timestamp. This keeps blocking probes
            // fast without finishing transactions; the probe restores the timestamp.
            // Reads and scenario actions run sequentially.
            auto* lock = row.BeginLocks(Tablet_->GetPhysicalSchema()->GetKeyColumnCount()) + conflict.LockIndex;
            ProbeLocks_.emplace_back(lock, lock->PrepareTimestamp.exchange(NotPreparedTimestamp));

            return TSortedDynamicStore::TRowBlockedWaitingResult();
        }));
    }

    std::string Describe(const TSequence& sequence) const
    {
        std::string result;
        for (int index = 0; index < std::ssize(sequence); ++index) {
            const auto& action = sequence[index];
            result += Format(
                "\n%v: %v %v {%v} locks=%v",
                index + 1,
                action.Kind,
                action.Transaction,
                action.Row,
                action.Locks);
        }

        return result;
    }

    TUnversionedOwningRow DecodeRow(const TAction& action)
    {
        auto row = BuildRow(action.Row);
        YT_VERIFY(action.Kind != "Lock" || row.GetCount() == 1);

        TUnversionedOwningRowBuilder builder;
        for (auto value : row) {
            if (action.Kind == "Add" && value.Id == CounterColumnId) {
                value.Flags |= EValueFlags::Aggregate;
            }

            builder.AddValue(value);
        }

        return builder.FinishRow();
    }

    // Read locks never block reads. Writes promote their columns to Exclusive
    // unless SharedWrite was explicitly requested; deletes lock the primary group.
    std::array<ELockType, 4> DecodeLocks(const TTransactionState& transaction) const
    {
        std::array<ELockType, 4> result{};
        for (const auto& [group, type] : transaction.Action.Locks) {
            result[GetOrCrash(LockGroups_, group)] = type;
        }

        if (transaction.Action.Kind == "Delete") {
            result[0] = ELockType::Exclusive;
        } else if (transaction.Action.Kind != "Lock") {
            for (const auto& value : transaction.Row) {
                if (value.Id != KeyColumnId && result[ColumnGroups[value.Id]] != ELockType::SharedWrite) {
                    result[ColumnGroups[value.Id]] = ELockType::Exclusive;
                }
            }
        }

        return result;
    }

    // Replay committed commands, ignoring versions outside the read interval.
    // A write of nulls still creates a row; a pure lock never does.
    TUnversionedOwningRow ComputeExpectedRow(
        i64 key,
        const TRead& read,
        const TProjection& projection,
        std::optional<int> store = std::nullopt) const
    {
        bool exists = false;
        std::array<std::optional<i64>, ColumnCount> values{};
        for (const auto* event : History_) {
            if (event->Row[KeyColumnId].Data.Int64 != key || event->Commit > read.Timestamp ||
                event->Commit < read.Retention ||
                (store && event->Store != *store && event->CommitStore != *store))
            {
                continue;
            }

            if (event->Action.Kind == "Delete") {
                exists = false;
                values = {};
            } else if (event->Action.Kind != "Lock") {
                exists = true;
                for (int index = 1; index < static_cast<int>(event->Row.GetCount()); ++index) {
                    const auto& value = event->Row[index];
                    auto& target = values[value.Id];
                    auto update = value.Type == EValueType::Null ? std::nullopt : std::optional(value.Data.Int64);

                    if (event->Action.Kind == "Add" && value.Id == CounterColumnId) {
                        if (update) {
                            target = target.value_or(0) + *update;
                        }
                    } else {
                        target = update;
                    }
                }
            }
        }

        if (!exists) {
            return {};
        }

        auto columns = projection.Columns.empty()
            ? std::vector<int>{KeyColumnId, 1, CounterColumnId, 3, 4}
            : projection.Columns;
        TUnversionedOwningRowBuilder builder;
        for (int column : columns) {
            builder.AddValue(
                column == KeyColumnId
                    ? MakeUnversionedInt64Value(key, KeyColumnId)
                    : (values[column]
                        ? MakeUnversionedInt64Value(*values[column], column)
                        : MakeUnversionedNullValue(column)));
        }

        return builder.FinishRow();
    }

    bool ShouldWait(
        i64 key,
        TStringBuf reader,
        const TRead& read,
        const TProjection& projection) const
    {
        if (read.Timestamp == AsyncLastCommittedTimestamp) {
            return false;
        }

        for (const auto& [name, transaction] : Transactions_) {
            if (transaction.Finished || transaction.Prepare == NullTimestamp ||
                transaction.Prepare >= read.Timestamp || transaction.Row[KeyColumnId].Data.Int64 != key)
            {
                continue;
            }

            // All groups are checked in these cases:
            // - Versioned reads fetch all columns before applying projection.
            // - An empty projection requests all columns.
            // - Retention may discard the writes proving existence; the check stays
            //   conservative even with retained writes (see ColumnFilterOrUniversal).
            // - No committed existence in this store: an unselected write may insert
            //   or resurrect the row. Existence in another store does not narrow the check.
            bool allGroups = reader == "VersionedLookup" || reader == "VersionedRange" ||
                projection.Columns.empty() || read.Retention != NullTimestamp ||
                !ComputeExpectedRow(key, {"LocalHistory", read.Timestamp}, projection, transaction.Store);

            auto locks = DecodeLocks(transaction);
            for (int group = 0; group < std::ssize(locks); ++group) {
                bool blocksReads = locks[group] == ELockType::Exclusive || locks[group] == ELockType::SharedWrite;
                bool selected = allGroups || group == 0 || std::ranges::any_of(
                    projection.Columns,
                    [&] (int column) {
                        return column != KeyColumnId && ColumnGroups[column] == group;
                    });

                if (blocksReads && selected) {
                    return true;
                }
            }
        }

        return false;
    }

    // Decode one action and apply it. Model timestamps and store generations come
    // from the sequence, not from descriptors observed in the real store.
    void Apply(const TAction& action, TTimestamp timestamp)
    {
        if (action.Kind == "Rotate") {
            RotateStores();
            ++CurrentStore_;
            InstallProbe();
        } else if (action.Kind == "Write" || action.Kind == "Add" ||
            action.Kind == "Lock" || action.Kind == "Delete")
        {
            auto row = DecodeRow(action);
            TLockMask lockMask;
            for (const auto& [group, type] : action.Locks) {
                lockMask.Set(GetOrCrash(LockGroups_, group), type);
            }

            auto transaction = StartTransaction(timestamp);
            auto rowRef = action.Kind == "Delete"
                ? DeleteRow(transaction.get(), row, /*prelock*/ true)
                : WriteAndLockRow(transaction.get(), row, lockMask, /*prelock*/ true);
            THROW_ERROR_EXCEPTION_IF(!rowRef, "Cannot acquire locks for transaction %Qv", action.Transaction);

            ConfirmRow(transaction.get(), rowRef);
            auto emplaced = Transactions_.emplace(
                action.Transaction,
                TTransactionState{
                    .Action = action,
                    .Row = row,
                    .Store = CurrentStore_,
                    .Actual = std::move(transaction),
                    .RowRef = rowRef,
                }).second;
            YT_VERIFY(emplaced);
        } else {
            auto& state = GetOrCrash(Transactions_, action.Transaction);
            YT_VERIFY(!state.Finished);

            if (action.Kind == "Prepare") {
                PrepareTransaction(state.Actual.get(), timestamp);
                PrepareRow(state.Actual.get(), state.RowRef);
                state.Prepare = timestamp;
            } else if (action.Kind == "Commit") {
                YT_VERIFY(state.Prepare != NullTimestamp);
                CommitTransaction(state.Actual.get(), timestamp);

                if (state.Action.Kind == "Delete") {
                    CommitRow(state.Actual.get(), TDeleteRowCommand{.Row = state.Row}, state.RowRef);
                } else {
                    CommitRow(
                        state.Actual.get(),
                        TWriteAndLockRowCommand{.Row = state.Row, .LockMask = state.RowRef.LockMask},
                        state.RowRef);
                }

                state.Finished = true;
                state.Commit = timestamp;
                state.CommitStore = CurrentStore_;
                History_.push_back(&state);
            } else {
                YT_VERIFY(action.Kind == "Abort");
                AbortTransaction(state.Actual.get());
                AbortRow(state.Actual.get(), state.RowRef);
                state.Finished = true;
            }
        }
    }

    void RunCase(const TSequence& sequence)
    {
        SCOPED_TRACE(Describe(sequence));

        // 1. Start from an empty tablet and reserve deterministic timestamps for all actions.
        History_.clear();
        Transactions_.clear();
        CurrentStore_ = 0;
        StoreManager_.Reset();
        CreateTablet();
        InstallProbe();

        // Also clean up shortened or failing sequences, without altering any read probe.
        auto abortTransactionsGuard = Finally([&] {
            for (const auto& [name, state] : Transactions_) {
                if (!state.Finished) {
                    Apply({"Abort", name}, /*timestamp*/ NullTimestamp);
                }
            }
        });

        std::vector<TTimestamp> timestamps;
        THashSet<i64> keys;
        auto before = GenerateTimestamp();
        auto seedTimestamp = before;
        for (const auto& action : sequence) {
            timestamps.push_back(GenerateTimestamp());

            if (!action.Row.empty()) {
                keys.insert(DecodeRow(action)[KeyColumnId].Data.Int64);
            }

            if (action.Kind == "Commit" && action.Transaction == "Seed") {
                seedTimestamp = timestamps.back();
            }
        }

        auto horizon = GenerateTimestamp();

        // 2. Check the empty prefix and every state reached by executing one more action.
        for (int prefix = 0; prefix <= std::ssize(sequence); ++prefix) {
            SCOPED_TRACE(Format("Prefix=%v/%v", prefix, sequence.size()));

            if (prefix != 0) {
                ASSERT_NO_THROW(Apply(sequence[prefix - 1], timestamps[prefix - 1]));
            }

            const TRead reads[] = {
                {.Name = "Snapshot", .Timestamp = horizon},
                {.Name = "AsyncLatest", .Timestamp = AsyncLastCommittedTimestamp},
                {.Name = "AtStep", .Timestamp = prefix == 0 ? before : timestamps[prefix - 1]},
                {
                    .Name = "RetentionBeforeWrite",
                    .Timestamp = horizon,
                    .Retention = TTimestamp(seedTimestamp.Underlying() - 1),
                },
                {
                    .Name = "RetentionAtWrite",
                    .Timestamp = horizon,
                    .Retention = seedTimestamp,
                },
                {
                    .Name = "RetentionAfterWrite",
                    .Timestamp = horizon,
                    .Retention = TTimestamp(seedTimestamp.Underlying() + 1),
                },
            };

            // 3. Record waiting but let reads finish immediately; restore all lock
            // timestamps before the next probe or action, including on exceptions.
            for (i64 key : keys) {
                for (auto reader : {"Lookup", "Range", "Scan", "VersionedLookup", "VersionedRange"}) {
                    for (const auto& read : reads) {
                        for (const auto& projection : Projections_) {
                            SCOPED_TRACE(Format(
                                "Key=%v Reader=%v Read=%v Projection=%v Timestamp=%v Retention=%v",
                                key,
                                reader,
                                read.Name,
                                projection.Name,
                                read.Timestamp,
                                read.Retention));

                            auto shouldWait = ShouldWait(key, reader, read, projection);
                            auto restoreLocksGuard = Finally([&] {
                                for (const auto& [lock, timestamp] : ProbeLocks_) {
                                    lock->PrepareTimestamp.store(timestamp);
                                }

                                ProbeLocks_.clear();
                            });

                            TUnversionedOwningRow row;
                            ASSERT_NO_THROW(row = DoLockProjection(
                                reader,
                                BuildKey(ToString(key)),
                                read.Timestamp,
                                read.Retention,
                                projection.Columns));
                            EXPECT_EQ(shouldWait, !ProbeLocks_.empty());
                            EXPECT_EQ(ComputeExpectedRow(key, read, projection), row);

                            if (HasFailure()) {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    // Independent groups cover different lock modes. SharedWrite increments
    // also cover several compatible writers in the same group.
    static std::vector<std::vector<ELockType>> GenerateLockSets(TStringBuf kind)
    {
        // A delete owns the whole row; two increments may share the counter lock.
        if (kind == "Delete") {
            return {{ELockType::Exclusive}};
        }

        if (kind == "Add") {
            return {{ELockType::SharedWrite}, {ELockType::SharedWrite, ELockType::SharedWrite}};
        }

        // Writes promote None and read locks on their columns to Exclusive.
        // Cover those lock types in pure-lock scenarios, without repeating equivalent writes.
        auto lockTypes = kind == "Write"
            ? std::vector{ELockType::Exclusive, ELockType::SharedWrite}
            : std::vector(std::begin(LockTypes), std::end(LockTypes));
        std::vector<std::vector<ELockType>> result;
        for (auto first : lockTypes) {
            result.push_back({first});
            for (auto second : lockTypes) {
                result.push_back({first, second});
            }
        }

        return result;
    }

    TSequence GenerateSequence(
        TStringBuf history,
        TStringBuf placement,
        TStringBuf kind,
        const std::vector<ELockType>& lockTypes,
        TWriterMask commitMask,
        TStringBuf schedule,
        bool reverse)
    {
        TSequence result;
        if (history != "Missing") {
            result.push_back({
                .Kind = "Write",
                .Transaction = "Seed",
                .Row = history == "Nulls"
                    ? "k=1;v=#;counter=#;other=#;p=#"
                    : "k=1;v=7;counter=10;other=11;p=12",
            });
            result.push_back({"Prepare", "Seed"});
            result.push_back({"Commit", "Seed"});
        }

        if (history == "Deleted") {
            result.push_back({"Delete", "DeletedSeed", "k=1"});
            result.push_back({"Prepare", "DeletedSeed"});
            result.push_back({"Commit", "DeletedSeed"});
        }

        if (placement == "HistoryPassive") {
            result.push_back({"Rotate"});
        }

        int count = std::ssize(lockTypes);
        std::vector<TSequence> streams;
        for (int index = 0; index < count; ++index) {
            auto name = Format("T%v", index + 1);
            auto column = kind == "Add" || index == 0 ? "counter" : "v";
            auto row = kind == "Lock" || kind == "Delete" ? "k=1" : Format("k=1;%v=5", column);
            TSequence stream = {{kind, name, row}, {"Prepare", name}};
            if (kind != "Delete") {
                stream.front().Locks.emplace_back(column, lockTypes[index]);
            }

            if (placement == "PreparedPassive") {
                stream.push_back({"Rotate"});
            }

            stream.push_back({commitMask & (1U << index) ? "Commit" : "Abort", name});
            streams.push_back(std::move(stream));
        }

        if (reverse && schedule != "Together") {
            std::reverse(streams.begin(), streams.end());
        }

        // Each stream preserves write -> prepare -> optional rotation -> finish.
        // Vary scheduling independently of lock strengths and transaction outcomes.
        std::vector<int> positions(count);
        auto append = [&] (int index, int steps) {
            while (steps-- > 0 && positions[index] < std::ssize(streams[index])) {
                result.push_back(streams[index][positions[index]++]);
            }
        };

        int steps = std::ssize(streams.front());
        if (schedule == "Together") {
            // All transactions overlap; check both completion orders.
            for (int step = 0; step < steps; ++step) {
                for (int index = 0; index < count; ++index) {
                    append(reverse && step == steps - 1 ? count - index - 1 : index, /*steps*/ 1);
                }
            }
        } else {
            YT_VERIFY(schedule == "Staggered");
            if (count > 1) {
                // Prepare one transaction before starting the other; then rotate/finish
                // the first while the second is still active in its original store.
                append(/*index*/ 0, /*steps*/ 2);
                append(/*index*/ 1, /*steps*/ 1);
            }

            for (int index = 0; index < count; ++index) {
                append(index, steps);
            }
        }

        return result;
    }
};

TEST_P(TLockVisibilityBoundaryTest, Matrix)
{
    const auto& [history, placement, kind, firstLockType] = GetParam();
    int cases = 0;
    for (const auto& lockTypes : GenerateLockSets(kind)) {
        if (lockTypes.front() != firstLockType) {
            continue;
        }

        for (TWriterMask commitMask = 0; commitMask < (1U << lockTypes.size()); ++commitMask) {
            for (TStringBuf schedule : {"Together", "Staggered"}) {
                for (bool reverse : {false, true}) {
                    if (lockTypes.size() == 1 && (schedule != "Together" || reverse)) {
                        continue;
                    }

                    SCOPED_TRACE(Format("Schedule=%v Reverse=%v", schedule, reverse));

                    RunCase(GenerateSequence(history, placement, kind, lockTypes, commitMask, schedule, reverse));
                    ++cases;

                    if (HasFailure()) {
                        return;
                    }
                }
            }
        }
    }

    const THashMap<TStringBuf, int> expectedCases = {
        {"Lock", 82},
        {"Write", 34},
        {"Add", 18},
        {"Delete", 2},
    };

    EXPECT_EQ(GetOrCrash(expectedCases, kind), cases);
}

TEST_F(TLockVisibilityBoundaryTest, MixedTransactionsAndResurrectionSequence)
{
    RunCase({
        {"Write", "Seed", "k=1;v=7;counter=10;other=11;p=12"},
        {"Prepare", "Seed"},
        {"Commit", "Seed"},
        {"Write", "T1", "k=1;v=20", {{"v", ELockType::Exclusive}}},
        {"Add", "T5", "k=1;counter=5", {{"counter", ELockType::SharedWrite}}},
        {"Lock", "T3", "k=1", {{"other", ELockType::SharedStrong}}},
        {"Prepare", "T5"},
        {"Rotate"},
        {"Prepare", "T1"},
        {"Prepare", "T3"},
        {"Abort", "T1"},
        {"Commit", "T3"},
        {"Commit", "T5"},
        {"Delete", "D", "k=1"},
        {"Prepare", "D"},
        {"Commit", "D"},
        {"Write", "Resurrect", "k=1;p=#"},
        {"Prepare", "Resurrect"},
        {"Commit", "Resurrect"},
    });
}

TEST_F(TLockVisibilityBoundaryTest, MultiColumnAndMultiKeySequence)
{
    RunCase({
        {"Write", "Seed", "k=1;v=7;counter=10;other=11;p=12"},
        {"Prepare", "Seed"},
        {"Commit", "Seed"},
        {"Write", "T1", "k=1;v=20;other=21", {{"v", ELockType::SharedWrite}, {"counter", ELockType::SharedWeak}}},
        {"Lock", "T5", "k=2", {{"primary", ELockType::SharedStrong}}},
        {"Prepare", "T1"},
        {"Prepare", "T5"},
        {"Rotate"},
        {"Abort", "T5"},
        {"Commit", "T1"},
        {"Add", "NullIncrement", "k=1;counter=#", {{"counter", ELockType::SharedWrite}}},
        {"Prepare", "NullIncrement"},
        {"Commit", "NullIncrement"},
        {"Write", "OtherKey", "k=2;p=42"},
        {"Prepare", "OtherKey"},
        {"Commit", "OtherKey"},
    });
}

TEST_F(TLockVisibilityBoundaryTest, PrimaryLockTypesAndOutcomesMatrix)
{
    for (auto type : LockTypes) {
        for (TStringBuf outcome : {"AbortActive", "AbortPrepared", "Commit"}) {
            TSequence sequence = {
                {"Write", "Seed", "k=1;v=7;counter=10"},
                {"Prepare", "Seed"},
                {"Commit", "Seed"},
                {"Lock", "T1", "k=1", {{"primary", type}}},
            };

            if (outcome != "AbortActive") {
                sequence.push_back({"Prepare", "T1"});
            }

            sequence.push_back({"Rotate"});
            sequence.push_back({outcome == "Commit" ? "Commit" : "Abort", "T1"});

            RunCase(sequence);

            if (HasFailure()) {
                return;
            }
        }
    }
}

// Each parameterized test covers one history, placement, operation and first lock type.
// FORK_SUBTESTS distributes these small partitions between test processes.
INSTANTIATE_TEST_SUITE_P(
    Missing,
    TLockVisibilityBoundaryTest,
    ::testing::ValuesIn(TLockVisibilityBoundaryTest::GenerateMatrixCases("Missing")),
    TLockVisibilityBoundaryTest::FormatCaseName);

INSTANTIATE_TEST_SUITE_P(
    Live,
    TLockVisibilityBoundaryTest,
    ::testing::ValuesIn(TLockVisibilityBoundaryTest::GenerateMatrixCases("Live")),
    TLockVisibilityBoundaryTest::FormatCaseName);

INSTANTIATE_TEST_SUITE_P(
    Nulls,
    TLockVisibilityBoundaryTest,
    ::testing::ValuesIn(TLockVisibilityBoundaryTest::GenerateMatrixCases("Nulls")),
    TLockVisibilityBoundaryTest::FormatCaseName);

INSTANTIATE_TEST_SUITE_P(
    Deleted,
    TLockVisibilityBoundaryTest,
    ::testing::ValuesIn(TLockVisibilityBoundaryTest::GenerateMatrixCases("Deleted")),
    TLockVisibilityBoundaryTest::FormatCaseName);

TEST_F(TLookupTest, TwoPartitions)
{
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        });
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=200> 1"),
        });
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=100> 2"),
        });

    TableSettings_.MountConfig->MinPartitionDataSize = 1;

    OnChunkStoresAdded();

    EXPECT_EQ(2, std::ssize(Tablet_->PartitionList()));

    ValidateLookup(
        {YsonToKey("0"), YsonToKey("1")},
        {BuildRow("k=0;v=1"), BuildRow("k=1;v=2")});
}

TEST_F(TLookupTest, WaitForPrefetchedPartitionStoreSessions)
{
    // Put the keys into two partitions to verify that the prefetched second
    // partition is not read before its store sessions finish opening.
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        });

    // With this two futures keep the prefetched store from finishing its asynchronous Open.
    auto metaReadStartedPromise = NewPromise<void>();
    auto metaReadAllowToProceedPromise = NewPromise<void>();
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=100> 1"),
        },
        /*chunkSchema*/ nullptr,
        metaReadAllowToProceedPromise.ToFuture(),
        metaReadStartedPromise);

    TableSettings_.MountConfig->MinPartitionDataSize = 1;
    TableSettings_.MountConfig->EnableHashChunkIndexForLookup = true;
    TableSettings_.MountConfig->PartitionReaderPrefetchKeyLimit = 2;

    OnChunkStoresAdded();

    EXPECT_EQ(2, std::ssize(Tablet_->PartitionList()));

    auto key0 = YsonToKey("0");
    auto key1 = YsonToKey("1");
    auto lookupRowsFuture = StartDoLookupRows({key0, key1});

    // The lookup must wait for the prefetched store session to finish opening.
    WaitFor(metaReadStartedPromise.ToFuture()).ThrowOnError();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    EXPECT_FALSE(lookupRowsFuture.IsSet());

    // Complete metadata loading and let the lookup finish.
    metaReadAllowToProceedPromise.Set();
    EXPECT_EQ(
        (std::vector{BuildRow("k=0;v=0"), BuildRow("k=1;v=1")}),
        WaitFor(lookupRowsFuture).ValueOrThrow());
}

TEST_F(TLookupTest, VersionedLookup)
{
    auto row = YsonToVersionedRow(
        "<id=0> 0",
        "<id=1;ts=100> 0; <id=1;ts=200> 1");

    AddChunkStore(/*eden*/ false, {row});

    OnChunkStoresAdded();

    EXPECT_EQ(
        ToString(DoVersionedLookupRow(BuildRow("k=0"))),
        ToString(row));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCachedRowTest, BlobDataSizeDoesNotOverflowInt32)
{
    constexpr ui32 PerValueLength = 1'500'000'000u;
    constexpr i64 ExpectedTotalBlob = static_cast<i64>(PerValueLength) * 2;
    static_assert(ExpectedTotalBlob > std::numeric_limits<i32>::max());

    auto buffer = New<TRowBuffer>();
    auto row = TMutableVersionedRow::Allocate(
        buffer->GetPool(),
        /*keyCount*/ 1,
        /*valueCount*/ 2,
        /*writeTimestampCount*/ 1,
        /*deleteTimestampCount*/ 0);

    char fakeBlob = '\0';

    const auto rowTimestamp = TTimestamp(10);

    row.BeginKeys()[0] = MakeUnversionedInt64Value(42, /*id*/ 0);

    for (int index = 0; index < 2; ++index) {
        auto& value = row.BeginValues()[index];
        value = MakeVersionedStringValue(TStringBuf(&fakeBlob, 1), rowTimestamp, /*id*/ 1);
        value.Length = PerValueLength;
    }

    row.BeginWriteTimestamps()[0] = rowTimestamp;

    auto nodeMemoryTracker = CreateNodeMemoryTracker(/*totalLimit*/ 1_MB, New<TNodeMemoryTrackerConfig>());
    auto tracker = nodeMemoryTracker->WithCategory(EMemoryCategory::LookupRowsCache);
    TSlabAllocator allocator(/*profiler*/ {}, tracker);

    auto cached = CachedRowFromVersionedRow(&allocator, row, /*retainedTimestamp*/ NullTimestamp);

    EXPECT_FALSE(cached) << "Allocation should fail cleanly, not crash";

    nodeMemoryTracker->ClearTrackers();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode
