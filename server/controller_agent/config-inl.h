#pragma once

#ifndef CONFIG_INL_H_
#error "Direct inclusion of this file is not allowed, include config.h"
// For the sake of sane code completion.
#include "config.h"
#endif

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TControllerAgentConfig::UpdateOptions(TOptions* options, NYT::NYTree::INodePtr patch)
{
    using NYTree::INodePtr;
    using NYTree::ConvertTo;

    if (!patch) {
        return;
    }

    if (*options) {
        *options = ConvertTo<TOptions>(PatchNode(patch, ConvertTo<INodePtr>(*options)));
    } else {
        *options = ConvertTo<TOptions>(patch);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

