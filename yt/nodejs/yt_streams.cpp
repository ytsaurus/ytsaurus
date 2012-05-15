#include "common.h"
#include "input_stream.h"
#include "output_stream.h"

// TODO(sandello): Remove this.
#ifndef _GLIBCXX_PURE
#define _GLIBCXX_PURE inline
#endif

namespace NYT {

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

void ExportYTStreams(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    ExportInputStream(target);
    ExportOutputStream(target);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

NODE_MODULE(yt_streams, NYT::ExportYTStreams)
