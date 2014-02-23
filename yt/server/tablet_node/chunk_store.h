#pragma once

#include "public.h"
#include "store_detail.h"

#include <core/misc/nullable.h>

#include <core/rpc/public.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TChunkStore
    : public TStoreBase
{
public:
    TChunkStore(
        TTabletManagerConfigPtr config,
        const TStoreId& id,
        TTablet* tablet,
        const NChunkClient::NProto::TChunkMeta* chunkMeta,
        NChunkClient::IBlockCachePtr blockCache,
        NRpc::IChannelPtr masterChannel,
        const TNullable<NNodeTrackerClient::TNodeDescriptor>& localDescriptor);
    ~TChunkStore();

    // IStore implementation.
    virtual i64 GetDataSize() const override;

    virtual NVersionedTableClient::TOwningKey GetMinKey() const override;
    virtual NVersionedTableClient::TOwningKey GetMaxKey() const override;

    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        NVersionedTableClient::TOwningKey lowerKey,
        NVersionedTableClient::TOwningKey upperKey,
        TTimestamp timestamp,
        const NVersionedTableClient::TColumnFilter& columnFilter) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) override;

private:
    TTabletManagerConfigPtr Config_;
    NChunkClient::IBlockCachePtr BlockCache_;
    NRpc::IChannelPtr MasterChannel_;
    TNullable<NNodeTrackerClient::TNodeDescriptor> LocalDescriptor_;

    // Cached for fast retrieval from ChunkMeta_.
    NVersionedTableClient::TOwningKey MinKey_;
    NVersionedTableClient::TOwningKey MaxKey_;
    i64 DataSize_;

    NChunkClient::NProto::TChunkMeta ChunkMeta_;

    NChunkClient::IAsyncReaderPtr ChunkReader_;
    NVersionedTableClient::TCachedVersionedChunkMetaPtr CachedMeta_;


    void PrecacheProperties();

};

DEFINE_REFCOUNTED_TYPE(TChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
