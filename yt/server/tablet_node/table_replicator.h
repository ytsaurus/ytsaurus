#pragma once

#include "public.h"

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/api/native/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTableReplicator
    : public TRefCounted
{
public:
    TTableReplicator(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        TTableReplicaInfo* replicaInfo,
        NApi::NNative::IConnectionPtr localConnection,
        TTabletSlotPtr slot,
        TSlotManagerPtr slotManager,
        IInvokerPtr workerInvoker);
    ~TTableReplicator();

    void Enable();
    void Disable();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTableReplicator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
