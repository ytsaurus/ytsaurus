#ifndef RESOURCE_TREE_ELEMENT_INL_H_
#error "Direct inclusion of this file is not allowed, include resource_tree_element.h"
// For the sake of sane code completion.
#include "resource_tree_element.h"
#endif

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

bool TResourceTreeElement::GetAlive() const
{
    return Alive_.load(std::memory_order::relaxed);
}

void TResourceTreeElement::SetNonAlive()
{
    // We need a barrier to be sure that nobody tries to change some usages.
    auto guard = WriterGuard(ResourceUsageLock_);

    Alive_ = false;
}

const TString& TResourceTreeElement::GetId()
{
    return Id_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
