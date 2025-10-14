#ifndef CHUNK_LOCATION_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_location.h"
// For the sake of sane code completion.
#include "chunk_location.h"
#endif

namespace NYT::NNode {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TCallback<T()> TChunkLocationBase::DisableOnError(const TCallback<T()> callback)
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

} // namespace NYT::NNode
