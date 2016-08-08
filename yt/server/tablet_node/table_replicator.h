#pragma once

#include "public.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTableReplicator
    : public TRefCounted
{
public:
    TTableReplicator();
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
