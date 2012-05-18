#include "common.h"
#include "input_stream.h"
#include "output_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

void ExportYTStreams(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TNodeJSInputStream::Initialize(target);
    TNodeJSOutputStream::Initialize(target);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

NODE_MODULE(ytnode_streams, NYT::ExportYTStreams)
