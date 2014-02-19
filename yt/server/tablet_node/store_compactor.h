#pragma once

#include "public.h"

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactor
    : public TRefCounted
{
public:
    TStoreCompactor(
        TStoreCompactorConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TStoreCompactor();

    void Start();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TStoreCompactor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
