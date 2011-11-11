#pragma once

#include "common.h"
#include "cypress_manager.h"

#include "../meta_state/meta_state_manager.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

// TOOD: this is a really stupid way of doing this
// We may need to fix it soon.
class TWorldInitializer
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TWorldInitializer> TPtr;

    TWorldInitializer(
        NMetaState::TMetaStateManager* metaStateManager,
        TCypressManager* cypressManager);

    void Start();

private:
    NMetaState::TMetaStateManager::TPtr MetaStateManager;
    TCypressManager::TPtr CypressManager;

    void CheckWorldInitialized();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

