#pragma once

#include "public.h"

#include <core/misc/small_vector.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/chunk_client/chunk.pb.h>

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
    explicit TTabletManager(
        TTabletManagerConfigPtr config,
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap);
    ~TTabletManager();

    void Initialize();

    TTablet* GetTabletOrThrow(const TTabletId& id);
    void ValidateTabletMounted(TTablet* tablet);

    void Read(
        TTablet* tablet,
        NTransactionClient::TTimestamp timestamp,
        const Stroka& encodedRequest,
        Stroka* encodedResponse);

    void Write(
        TTablet* tablet,
        TTransaction* transaction,
        const Stroka& encodedRequest);


    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
