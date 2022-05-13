#pragma once

#include <yt/yt/server/node/tablet_node/lookup.h>
#include <yt/yt/server/node/tablet_node/tablet.h>
#include <yt/yt/server/node/tablet_node/store_detail.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

struct TChunkData
{
    std::vector<TSharedRef> Blocks;
    TRefCountedChunkMetaPtr Meta;
};

class TMockChunkReader
    : public IChunkReader
{
public:
    TMockChunkReader(TChunkId chunkId, TChunkData chunkData)
        : ChunkId_(chunkId)
        , ChunkData_(std::move(chunkData))
    { }

    // IChunkReader implementation.
    TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& /*options*/,
        const std::vector<int>& blockIndexes,
        std::optional<i64> /*estimatedSize*/) override
    {
        std::vector<TBlock> result;
        for (auto index : blockIndexes) {
            YT_VERIFY(index < std::ssize(ChunkData_.Blocks));
            result.emplace_back(ChunkData_.Blocks[index]);
        }
        return MakeFuture(result);
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& /*options*/,
        int /*firstBlockIndex*/,
        int /*blockCount*/,
        std::optional<i64> /*estimatedSize*/) override
    {
        YT_ABORT();
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& /*options*/,
        std::optional<int> /*partitionTag*/,
        const std::optional<std::vector<int>>& /*extensionTags*/) override
    {
        return MakeFuture(ChunkData_.Meta);
    }

    TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    TInstant GetLastFailureTime() const override
    {
        YT_ABORT();
    }

private:
    const TChunkId ChunkId_;
    const TChunkData ChunkData_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMockBackendChunkReadersHolder)

class TMockBackendChunkReadersHolder
    : public IBackendChunkReadersHolder
{
public:
    void RegisterBackendChunkReader(TChunkId chunkId, TChunkData chunkData)
    {
        EmplaceOrCrash(
            ChunkIdToReaders_,
            chunkId,
            New<TMockChunkReader>(chunkId, std::move(chunkData)));
    }

    // IBackendChunkReadersHolder implementation.
    TBackendReaders GetBackendReaders(
        TChunkStoreBase* owner,
        std::optional<EWorkloadCategory> /*workloadCategory*/) override
    {
        return TBackendReaders{
            .ChunkReader = GetOrCrash(ChunkIdToReaders_, owner->GetChunkId()),
            .LookupReader = nullptr,
            .ReaderConfig = ReaderConfig_
        };
    }

    NChunkClient::TChunkReplicaList GetReplicas(
        TChunkStoreBase* /*owner*/,
        NNodeTrackerClient::TNodeId /*localNodeId*/) const override
    {
        YT_ABORT();
    }

    void InvalidateCachedReadersAndTryResetConfig(
        const TTabletStoreReaderConfigPtr& /*config*/) override
    {
        YT_ABORT();
    }

    TTabletStoreReaderConfigPtr GetReaderConfig() override
    {
        return ReaderConfig_;
    }

private:
    const TTabletStoreReaderConfigPtr ReaderConfig_ = New<TTabletStoreReaderConfig>();

    THashMap<TChunkId, TIntrusivePtr<TMockChunkReader>> ChunkIdToReaders_;
};

DEFINE_REFCOUNTED_TYPE(TMockBackendChunkReadersHolder)

////////////////////////////////////////////////////////////////////////////////

inline TUnversionedOwningRow LookupRowImpl(
    TTablet* tablet,
    const TLegacyOwningKey& key,
    TTimestamp timestamp,
    const std::vector<int>& columnIndexes,
    TTabletSnapshotPtr tabletSnapshot,
    TClientChunkReadOptions chunkReadOptions = TClientChunkReadOptions())
{
    if (!tabletSnapshot) {
        tabletSnapshot = tablet->BuildSnapshot(nullptr);
    }

    TSharedRef request;
    {
        TReqLookupRows req;
        if (!columnIndexes.empty()) {
            ToProto(req.mutable_column_filter()->mutable_indexes(), columnIndexes);
        }
        std::vector<TUnversionedRow> keys(1, key);

        auto writer = CreateWireProtocolWriter();
        writer->WriteMessage(req);
        writer->WriteSchemafulRowset(keys);

        struct TMergedTag { };
        request = MergeRefsToRef<TMergedTag>(writer->Finish());
    }

    TSharedRef response;
    {
        auto reader = CreateWireProtocolReader(request);
        auto writer = CreateWireProtocolWriter();
        LookupRows(
            tabletSnapshot,
            TReadTimestampRange{
                .Timestamp = timestamp,
            },
            false,
            chunkReadOptions,
            reader.get(),
            writer.get());
        struct TMergedTag { };
        response = MergeRefsToRef<TMergedTag>(writer->Finish());
    }

    {
        auto reader = CreateWireProtocolReader(response);
        auto schemaData = IWireProtocolReader::GetSchemaData(*tablet->GetPhysicalSchema(), TColumnFilter());
        auto row = reader->ReadSchemafulRow(schemaData, false);
        return TUnversionedOwningRow(row);
    }
}

inline TVersionedOwningRow VersionedLookupRowImpl(
    TTablet* tablet,
    const TLegacyOwningKey& key,
    int minDataVersions = 100,
    TTimestamp timestamp = AsyncLastCommittedTimestamp,
    TClientChunkReadOptions chunkReadOptions = TClientChunkReadOptions())
{
    TSharedRef request;
    {
        TReqVersionedLookupRows req;
        std::vector<TUnversionedRow> keys(1, key);

        auto writer = CreateWireProtocolWriter();
        writer->WriteMessage(req);
        writer->WriteSchemafulRowset(keys);

        struct TMergedTag { };
        request = MergeRefsToRef<TMergedTag>(writer->Finish());
    }

    TSharedRef response;
    {
        auto retentionConfig = New<NTableClient::TRetentionConfig>();
        retentionConfig->MinDataVersions = minDataVersions;
        retentionConfig->MaxDataVersions = minDataVersions;

        auto reader = CreateWireProtocolReader(request);
        auto writer = CreateWireProtocolWriter();
        VersionedLookupRows(
            tablet->BuildSnapshot(nullptr),
            timestamp,
            false,
            chunkReadOptions,
            retentionConfig,
            reader.get(),
            writer.get());
        struct TMergedTag { };
        response = MergeRefsToRef<TMergedTag>(writer->Finish());
    }

    {
        auto reader = CreateWireProtocolReader(response);
        auto schemaData = IWireProtocolReader::GetSchemaData(*tablet->GetPhysicalSchema(), TColumnFilter());
        auto row = reader->ReadVersionedRow(schemaData, false);
        return TVersionedOwningRow(row);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
