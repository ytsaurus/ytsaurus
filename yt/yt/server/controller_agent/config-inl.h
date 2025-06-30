#ifndef CONFIG_INL_H_
#error "Direct inclusion of this file is not allowed, include config.h"
// For the sake of sane code completion.
#include "config.h"
#endif

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TControllerAgentConfig::BuildOptions(TOptions* options, NYTree::INodePtr optionsNode, NYTree::INodePtr patch)
{
    using NYTree::ConvertTo;

    if (!patch && !optionsNode) {
        *options = New<typename TOptions::TUnderlying>();
        return;
    }

    if (optionsNode) {
        if (patch) {
            optionsNode = PatchNode(patch, optionsNode);
        }
    } else {
        optionsNode = patch;
    }

    *options = ConvertTo<TOptions>(optionsNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

