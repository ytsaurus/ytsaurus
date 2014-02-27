#pragma once

#include "public.h"

#include <core/concurrency/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TPeerBlockUpdater
    : public TRefCounted
{
public:
    TPeerBlockUpdater(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    void Update();

    TDataNodeConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor;

};

DEFINE_REFCOUNTED_TYPE(TPeerBlockUpdater)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
