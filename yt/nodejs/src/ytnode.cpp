#include "common.h"
#include "input_stream.h"
#include "output_stream.h"
#include "driver.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

void ExportYT(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TNodeJSInputStream::Initialize(target);
    TNodeJSOutputStream::Initialize(target);

    TNodeJSDriver::Initialize(target);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

NODE_MODULE(ytnode, NYT::ExportYT)
