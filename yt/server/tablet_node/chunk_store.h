#pragma once

#include "public.h"
#include "store.h"

#include <core/misc/nullable.h>

#include <core/rpc/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TChunkStore
    : public IStore
{
public:
    TChunkStore(
        TTabletManagerConfigPtr config,
        const TStoreId& id,
        TTablet* tablet,
        NChunkClient::IBlockCachePtr blockCache,
        NRpc::IChannelPtr masterChannel,
        const TNullable<NNodeTrackerClient::TNodeDescriptor>& localDescriptor);
    ~TChunkStore();

    // IStore implementation.
    virtual TStoreId GetId() const override;

    virtual EStoreState GetState() const override;
    virtual void SetState(EStoreState state) override;

    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        NVersionedTableClient::TOwningKey lowerKey,
        NVersionedTableClient::TOwningKey upperKey,
        TTimestamp timestamp,
        const NApi::TColumnFilter& columnFilter) override;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) override;

private:
    TTabletManagerConfigPtr Config_;
    TStoreId Id_;
    TTablet* Tablet_;
    NChunkClient::IBlockCachePtr BlockCache_;
    NRpc::IChannelPtr MasterChannel_;
    TNullable<NNodeTrackerClient::TNodeDescriptor> LocalDescriptor_;

    EStoreState State_;

    NChunkClient::IAsyncReaderPtr ChunkReader_;
    NVersionedTableClient::TCachedVersionedChunkMetaPtr CachedMeta_;

};

DEFINE_REFCOUNTED_TYPE(TChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
