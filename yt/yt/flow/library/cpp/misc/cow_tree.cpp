#include "cow_tree.h"

namespace NYT::NFlow::NDetail {

////////////////////////////////////////////////////////////////////////////////

void TMaybeInplaceRefCounted::MaybeInplaceRef()
{
    RefCount_.fetch_add(1);
}

void TMaybeInplaceRefCounted::MaybeInplaceUnref()
{
    RefCount_.fetch_sub(1);
}

bool TMaybeInplaceRefCounted::IsSingleRef() const
{
    return RefCount_.load() == 1;
}

////////////////////////////////////////////////////////////////////////////////

void TCowTreeVersionContext::Increment()
{
    ++Version_;
}

size_t TCowTreeVersionContext::GetVersion() const
{
    return Version_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDetail
