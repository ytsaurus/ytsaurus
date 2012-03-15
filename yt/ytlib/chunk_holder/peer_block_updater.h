#pragma once

#include "public.h"

// TODO(babenko): replace with public.h
#include <ytlib/actions/invoker.h>
#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/cell_node/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderServiceProxy;

class TPeerBlockUpdater
    : public TRefCounted
{
public:
    TPeerBlockUpdater(
        TChunkHolderConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    void Update();
    
    typedef TChunkHolderServiceProxy TProxy;

    TChunkHolderConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;
    TPeriodicInvoker::TPtr PeriodicInvoker;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
