#pragma once

#include "public.h"

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages in-memory tables served by the node.
/*!
 *  Ensures that chunk stores of in-memory tables are preloaded when a node starts.
 *
 *  Provides means for intercepting data write-out during flushes and compactions
 *  and thus enable new chunk stores to be created with all blocks already resident.
 */
class TInMemoryManager
    : public TRefCounted
{
public:
    TInMemoryManager(
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TInMemoryManager();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TInMemoryManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
