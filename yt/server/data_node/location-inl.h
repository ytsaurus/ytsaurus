#pragma once
#ifndef LOCATION_INL_H_
#error "Direct inclusion of this file is not allowed, include location.h"
#endif

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TCallback<T()> TLocation::DisableOnError(const TCallback<T()> callback)
{
    return BIND([=, this_ = MakeStrong(this)] () {
        try {
            return callback.Run();
        } catch (const std::exception& ex) {
            Disable(ex);
            Y_UNREACHABLE(); // Disable() exits the process.
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
