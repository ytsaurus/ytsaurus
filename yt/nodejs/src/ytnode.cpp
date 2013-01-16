#include "common.h"
#include "error.h"
#include "node.h"
#include "input_stream.h"
#include "input_stub.h"
#include "output_stream.h"
#include "output_stub.h"
#include "driver.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

void ExportYT(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    InitializeCommon(target);
    InitializeError(target);

    TNodeJSNode::Initialize(target);

    TNodeJSInputStream::Initialize(target);
    TNodeJSOutputStream::Initialize(target);

    TInputStreamStub::Initialize(target);
    TOutputStreamStub::Initialize(target);

    TNodeJSDriver::Initialize(target);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT

NODE_MODULE(ytnode, NYT::NNodeJS::ExportYT)
