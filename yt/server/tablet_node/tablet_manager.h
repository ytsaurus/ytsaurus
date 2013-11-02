#pragma once

#include "public.h"

#include <ytlib/new_table_client/public.h>

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
    explicit TTabletManager(
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap);
    ~TTabletManager();

    TTablet* GetTabletOrThrow(const TTabletId& id);

    void Write(
        TTablet* tablet,
        TTransaction* transaction,
        NVersionedTableClient::IReaderPtr reader);

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
