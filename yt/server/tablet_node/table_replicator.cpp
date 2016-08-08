#include "table_replicator.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTableReplicator::TImpl
    : public TRefCounted
{
public:
    TImpl()
    {

    }

    void Enable()
    {

    }

    void Disable()
    {

    }

};

////////////////////////////////////////////////////////////////////////////////

TTableReplicator::TTableReplicator()
    : Impl_(New<TImpl>())
{ }

TTableReplicator::~TTableReplicator() = default;

void TTableReplicator::Enable()
{
    Impl_->Enable();
}

void TTableReplicator::Disable()
{
    Impl_->Disable();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
