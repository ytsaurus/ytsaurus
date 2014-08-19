#include "stdafx.h"
#include "chunk_store.h"
#include "tablet.h"
#include "config.h"
#include "automaton.h"

#include <core/concurrency/scheduler.h>

#include <core/ytree/fluent.h>

#include <core/misc/protobuf_helpers.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_chunk_reader.h>
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

class TChunkStore::TLookuper
    : public IVersionedLookuper
{
public:
    TLookuper(
        TChunkStorePtr store,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter)
        : Store_(std::move(store))
        , Timestamp_(timestamp)
        , ColumnFilter_(columnFilter)
    { }

    virtual TFuture<TErrorOr<TVersionedRow>> Lookup(TKey key) override
    {
        Reader_ = Store_->CreateReader(
            TOwningKey(key),
            GetKeySuccessor(key),
            Timestamp_,
            ColumnFilter_);
        if (!Reader_) {
            static auto NullRow = MakeFuture<TErrorOr<TVersionedRow>>(TVersionedRow());
            return NullRow;
        }

        return Reader_->Open().Apply(BIND(&TLookuper::OnOpened, MakeStrong(this)));
    }

private:
    TChunkStorePtr Store_;
    TTimestamp Timestamp_;
    TColumnFilter ColumnFilter_;

    IVersionedReaderPtr Reader_;
    std::vector<TVersionedRow> PooledRows_;


    TErrorOr<TVersionedRow> OnOpened(TError error)
    {
        if (!error.IsOK()) {
            return error;
        }

        PooledRows_.clear();
        PooledRows_.reserve(1);
        Reader_->Read(&PooledRows_);
        YASSERT(PooledRows_.size() <= 1);
        return PooledRows_.empty() ? TVersionedRow() : PooledRows_[0];
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
    , DataSize_(-1)
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

    auto asyncChunkReader = BIND(&CreateLocalChunkReader)
        .AsyncVia(Bootstrap_->GetControlInvoker())
        .Run(Bootstrap_, Id_);
    auto chunkReader = WaitFor(asyncChunkReader);
    if (!chunkReader) {
        // TODO(babenko): provide seed replicas
        chunkReader = CreateReplicationReader(
            Bootstrap_->GetConfig()->TabletNode->ChunkReader,
            Bootstrap_->GetBlockStore()->GetBlockCache(),
            Bootstrap_->GetMasterClient()->GetMasterChannel(),
            New<TNodeDirectory>(),
            Bootstrap_->GetLocalDescriptor(),
            Id_);
    }

    if (!CachedMeta_) {
        auto cachedMetaOrError = WaitFor(TCachedVersionedChunkMeta::Load(
            chunkReader,
            Tablet_->Schema(),
            Tablet_->KeyColumns()));
        THROW_ERROR_EXCEPTION_IF_FAILED(cachedMetaOrError);
        CachedMeta_ = cachedMetaOrError.Value();
    }

    TReadLimit lowerLimit;
    lowerLimit.SetKey(std::move(lowerKey));

    TReadLimit upperLimit;
    upperLimit.SetKey(std::move(upperKey));

    return CreateVersionedChunkReader(
        Bootstrap_->GetConfig()->TabletNode->ChunkReader,
        chunkReader,
        CachedMeta_,
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

    return New<TLookuper>(this, timestamp, columnFilter);
}

TTimestamp TChunkStore::GetLatestCommitTimestamp(TKey key)
{
    // TODO(babenko): fixme
    return NullTimestamp;
    
    if (key < MinKey_.Get() || key > MaxKey_.Get()) {
        return NullTimestamp;
    }

    auto lowerKey = TOwningKey(key);
    auto upperKey = GetKeySuccessor(lowerKey.Get());

    TColumnFilter columnFilter;
    columnFilter.All = false; // need no columns

    // TODO(babenko): limit the number of fetched versions to 1
    auto reader = CreateReader(
        lowerKey,
        upperKey,
        LastCommittedTimestamp,
        columnFilter);
    if (!reader) {
        return NullTimestamp;
    }

    {
        auto result = WaitFor(reader->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    PooledRows_.reserve(1);
    if (!reader->Read(&PooledRows_)) {
        return false;
    }

    auto row = PooledRows_[0];
    auto timestamp = row.GetLatestTimestamp();
    YASSERT(timestamp != NullTimestamp);
    return timestamp;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

