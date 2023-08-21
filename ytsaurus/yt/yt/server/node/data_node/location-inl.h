#ifndef LOCATION_INL_H_
#error "Direct inclusion of this file is not allowed, include location.h"
// For the sake of sane code completion.
#include "location.h"
#endif

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TCallback<T()> TChunkLocation::DisableOnError(const TCallback<T()> callback)
{
    return BIND([=, this, this_ = MakeStrong(this)] {
        try {
            return callback.Run();
        } catch (const std::exception& ex) {
            ScheduleDisable(ex);
            throw;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
