#include "state_manager_registry.h"

#include <bigrt/lib/processing/state_manager/base/manager.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void TStateManagerRegistry::Add(TString id, NBigRT::TBaseStateManagerPtr stateManager)
{
    const auto& [iter, inserted] = Table_.emplace(id, stateManager);
    YT_VERIFY(inserted);
}

const NBigRT::TBaseStateManagerPtr TStateManagerRegistry::GetBase(TString id) const
{
    auto iter = Table_.find(id);
    YT_VERIFY(iter != Table_.end());
    return iter->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
