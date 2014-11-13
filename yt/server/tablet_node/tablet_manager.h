#pragma once

#include "public.h"

#include <core/misc/small_vector.h>

#include <core/yson/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <ytlib/tablet_client/public.h>

#include <server/hydra/entity_map.h>

#include <server/cell_node/public.h>

#include <server/tablet_node/tablet_manager.pb.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTabletManager
    : public TRefCounted
{
public:
    TTabletManager(
        TTabletManagerConfigPtr config,
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap);
    ~TTabletManager();

    void Initialize();

    TTablet* GetTabletOrThrow(const TTabletId& id);
    void ValidateTabletMounted(TTablet* tablet);


    void BackoffStore(IStorePtr store, EStoreState state);


    void Read(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        NTabletClient::TWireProtocolReader* reader,
        NTabletClient::TWireProtocolWriter* writer);

    void Write(
        TTablet* tablet,
        TTransaction* transaction,
        NTabletClient::TWireProtocolReader* reader);


    IStorePtr CreateStore(TTablet* tablet, const TStoreId& storeId);
    void ScheduleStoreRotation(TTablet* tablet);

    void BuildOrchidYson(NYson::IYsonConsumer* consumer);


    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTabletManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
