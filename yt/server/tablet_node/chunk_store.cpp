#include "stdafx.h"
#include "chunk_store.h"
#include "tablet.h"
#include "config.h"
#include "automaton.h"
#include "transaction.h"

#include <core/concurrency/scheduler.h>
#include <core/concurrency/delayed_executor.h>

#include <core/ytree/fluent.h>

#include <core/misc/protobuf_helpers.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_chunk_reader.h>
#include <ytlib/new_table_client/versioned_lookuper.h>
#include <ytlib/new_table_client/versioned_chunk_lookuper.h>
#include <ytlib/new_table_client/cached_versioned_chunk_meta.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <ytlib/api/client.h>

#include <ytlib/chunk_client/reader.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <server/data_node/local_chunk_reader.h>
#include <server/data_node/block_store.h>
#include <server/data_node/chunk_registry.h>

#include <server/query_agent/config.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectClient;
using namespace NVersionedTableClient;
using namespace NVersionedTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NDataNode;
using namespace NCellNode;
using namespace NQueryAgent;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

static const TDuration ChunkExpirationTimeout = TDuration::Seconds(15);
static const TDuration ChunkReaderExpirationTimeout = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

class TChunkStore::TLocalChunkReaderWrapper
    : public NChunkClient::IReader
{
public:
    TLocalChunkReaderWrapper(
        NChunkClient::IReaderPtr underlyingReader,
        TChunkStorePtr owner)
        : UnderlyingReader_(std::move(underlyingReader))
        , Owner_(std::move(owner))
    { }

    virtual TAsyncReadBlocksResult ReadBlocks(const std::vector<int>& blockIndexes) override
    {
        auto this_ = MakeStrong(this);
        return UnderlyingReader_->ReadBlocks(blockIndexes).Apply(
            BIND([this, this_] (TReadBlocksResult result) -> TReadBlocksResult {
                CheckResult(result);
                return result;
            }));
    }

    virtual TAsyncReadBlocksResult ReadBlocks(int firstBlockIndex, int blockCount) override
    {
        auto this_ = MakeStrong(this);
        return UnderlyingReader_->ReadBlocks(firstBlockIndex, blockCount).Apply(
            BIND([this, this_] (TReadBlocksResult result) -> TReadBlocksResult {
                CheckResult(result);
                return result;
            }));
    }

    virtual TAsyncGetMetaResult GetMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* extensionTags = nullptr) override
    {
        auto this_ = MakeStrong(this);
        return UnderlyingReader_->GetMeta(partitionTag, extensionTags).Apply(
            BIND([this, this_] (TGetMetaResult result) -> TGetMetaResult {
                CheckResult(result);
                return result;
            }));
    }

    virtual TChunkId GetChunkId() const override
    {
        return UnderlyingReader_->GetChunkId();
    }

private:
    NChunkClient::IReaderPtr UnderlyingReader_;
    TChunkStorePtr Owner_;


    void CheckResult(const TError& result)
    {
        if (!result.IsOK()) {
            Owner_->OnLocalReaderFailed();
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(
    const TStoreId& id,
    TTablet* tablet,
    const TChunkMeta* chunkMeta,
    TBootstrap* boostrap)
    : TStoreBase(
        id,
        tablet)
    , Bootstrap_(boostrap)
{
    State_ = EStoreState::Persistent;

    if (chunkMeta) {
        ChunkMeta_ = *chunkMeta;
        PrecacheProperties();
    }

    YCHECK(
        TypeFromId(Id_) == EObjectType::Chunk ||
        TypeFromId(Id_) == EObjectType::ErasureChunk);
}

TChunkStore::~TChunkStore()
{ }

const TChunkMeta& TChunkStore::GetChunkMeta() const
{
    return ChunkMeta_;
}

void TChunkStore::SetBackingStore(IStorePtr store)
{
    BackingStore_ = store;
}

bool TChunkStore::HasBackingStore() const
{
    return BackingStore_ != nullptr;
}

EStoreType TChunkStore::GetType() const
{
    return EStoreType::Chunk;
}

i64 TChunkStore::GetDataSize() const
{
    return DataSize_;
}

TOwningKey TChunkStore::GetMinKey() const
{
    return MinKey_;
}

TOwningKey TChunkStore::GetMaxKey() const
{
    return MaxKey_;
}

TTimestamp TChunkStore::GetMinTimestamp() const
{
    return MinTimestamp_;
}

TTimestamp TChunkStore::GetMaxTimestamp() const
{
    return MaxTimestamp_;
}

IVersionedReaderPtr TChunkStore::CreateReader(
    TOwningKey lowerKey,
    TOwningKey upperKey,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    if (BackingStore_) {
        return BackingStore_->CreateReader(
            std::move(lowerKey),
            std::move(upperKey),
            timestamp,
            columnFilter);
    }

    if (upperKey <= MinKey_ || lowerKey > MaxKey_) {
        return nullptr;
    }

    auto chunk = PrepareChunk();
    auto chunkReader = PrepareChunkReader(chunk);
    auto cachedVersionedChunkMeta = PrepareCachedVersionedChunkMeta(chunkReader);

    TReadLimit lowerLimit;
    lowerLimit.SetKey(std::move(lowerKey));

    TReadLimit upperLimit;
    upperLimit.SetKey(std::move(upperKey));

    return CreateVersionedChunkReader(
        Bootstrap_->GetConfig()->TabletNode->ChunkReader,
        std::move(chunkReader),
        Bootstrap_->GetUncompressedBlockCache(),
        std::move(cachedVersionedChunkMeta),
        lowerLimit,
        upperLimit,
        columnFilter,
        timestamp);
}

IVersionedLookuperPtr TChunkStore::CreateLookuper(
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    if (BackingStore_) {
        return BackingStore_->CreateLookuper(timestamp, columnFilter);
    }

    auto chunk = PrepareChunk();
    auto chunkReader = PrepareChunkReader(chunk);
    auto cachedVersionedChunkMeta = PrepareCachedVersionedChunkMeta(chunkReader);

    return CreateVersionedChunkLookuper(
        Bootstrap_->GetConfig()->TabletNode->ChunkReader,
        std::move(chunkReader),
        Bootstrap_->GetUncompressedBlockCache(),
        std::move(cachedVersionedChunkMeta),
        columnFilter,
        timestamp);
}

void TChunkStore::CheckRowLocks(
    TKey key,
    TTransaction* transaction,
    ui32 lockMask)
{
    if (BackingStore_) {
        return BackingStore_->CheckRowLocks(key, transaction, lockMask);
    }

    THROW_ERROR_EXCEPTION(
        "Checking for transaction conflicts against chunk stores is not supported; "
        "consider reducing transaction duration or increasing store retention time")
        << TErrorAttribute("transaction_id", transaction->GetId())
        << TErrorAttribute("tablet_id", Tablet_->GetId())
        << TErrorAttribute("store_id", Id_)
        << TErrorAttribute("key", key);
}

void TChunkStore::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, GetPersistentState());
    Save(context, ChunkMeta_);
}

void TChunkStore::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, State_);
    Load(context, ChunkMeta_);

    PrecacheProperties();
}

void TChunkStore::BuildOrchidYson(IYsonConsumer* consumer)
{
    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    BuildYsonMapFluently(consumer)
        .Item("compressed_data_size").Value(miscExt.compressed_data_size())
        .Item("uncompressed_data_size").Value(miscExt.uncompressed_data_size())
        .Item("key_count").Value(miscExt.row_count())
        .Item("min_key").Value(MinKey_)
        .Item("max_key").Value(MaxKey_)
        .Item("min_timestamp").Value(MinTimestamp_)
        .Item("max_timestamp").Value(MaxTimestamp_)
        .DoIf(BackingStore_, [&] (TFluentMap fluent) {
            fluent.Item("backing_store_id").Value(BackingStore_->GetId());
        });
}

IChunkPtr TChunkStore::PrepareChunk()
{
    if (ChunkInitialized_) {
        return Chunk_;
    }

    auto chunkRegistry = Bootstrap_->GetChunkRegistry();
    auto asyncChunk = BIND(&TChunkRegistry::FindChunk, chunkRegistry, Id_)
        .AsyncVia(Bootstrap_->GetControlInvoker())
        .Run();
    auto chunk = Chunk_ = WaitFor(asyncChunk);
    ChunkInitialized_ = true;

    auto this_ = MakeStrong(this);
    TDelayedExecutor::Submit(
        BIND([this, this_] () {
            ChunkInitialized_ = false;
            Chunk_.Reset();
        }).Via(Tablet_->GetEpochAutomatonInvoker()),
        ChunkExpirationTimeout);

    return chunk;
}

IReaderPtr TChunkStore::PrepareChunkReader(IChunkPtr chunk)
{
    if (ChunkReader_) {
        return ChunkReader_;
    }

    IReaderPtr chunkReader;
    if (chunk) {
        auto localChunkReader = CreateLocalChunkReader(
            Bootstrap_,
            Bootstrap_->GetConfig()->TabletNode->ChunkReader,
            chunk);
        chunkReader = ChunkReader_ = New<TLocalChunkReaderWrapper>(
            localChunkReader,
            this);
    } else {
        // TODO(babenko): provide seed replicas
        chunkReader = ChunkReader_ = CreateReplicationReader(
            Bootstrap_->GetConfig()->TabletNode->ChunkReader,
            Bootstrap_->GetBlockStore()->GetCompressedBlockCache(),
            Bootstrap_->GetMasterClient()->GetMasterChannel(),
            New<TNodeDirectory>(),
            Bootstrap_->GetLocalDescriptor(),
            Id_);
    }

    auto this_ = MakeStrong(this);
    TDelayedExecutor::Submit(
        BIND([this, this_] () {
            ChunkReader_.Reset();
        }).Via(Tablet_->GetEpochAutomatonInvoker()),
        ChunkReaderExpirationTimeout);

    return chunkReader;
}

TCachedVersionedChunkMetaPtr TChunkStore::PrepareCachedVersionedChunkMeta(IReaderPtr chunkReader)
{
    if (CachedVersionedChunkMeta_) {
        return CachedVersionedChunkMeta_;
    }

    auto cachedMetaOrError = WaitFor(TCachedVersionedChunkMeta::Load(
        chunkReader,
        Tablet_->Schema(),
        Tablet_->KeyColumns()));
    THROW_ERROR_EXCEPTION_IF_FAILED(cachedMetaOrError);
    auto cachedMeta = CachedVersionedChunkMeta_ = cachedMetaOrError.Value();

    return cachedMeta;
}

void TChunkStore::PrecacheProperties()
{
    // Precache frequently used values.
    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    DataSize_ = miscExt.uncompressed_data_size();
    MinTimestamp_ = miscExt.min_timestamp();
    MaxTimestamp_ = miscExt.max_timestamp();

    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_.extensions());
    MinKey_ = FromProto<TOwningKey>(boundaryKeysExt.min());
    MaxKey_ = FromProto<TOwningKey>(boundaryKeysExt.max());
}

void TChunkStore::OnLocalReaderFailed()
{
    auto this_ = MakeStrong(this);
    Tablet_->GetEpochAutomatonInvoker()->Invoke(BIND([this, this_] () {
        ChunkInitialized_ = false;
        Chunk_.Reset();
        ChunkReader_.Reset();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

