#include "common.h"
#include "driver.h"
#include "error.h"
#include "future.h"
#include "input_stream.h"
#include "input_stub.h"
#include "node.h"
#include "output_stream.h"
#include "output_stub.h"

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

    TFutureWrap::Initialize(target);

    TNodeWrap::Initialize(target);

    TInputStreamWrap::Initialize(target);
    TOutputStreamWrap::Initialize(target);

    TInputStreamStub::Initialize(target);
    TOutputStreamStub::Initialize(target);

    TDriverWrap::Initialize(target);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT

#ifdef STATIC_NODE_EXTENSION
NODE_MODULE_X(ytnode, NYT::NNodeJS::ExportYT, nullptr, NM_F_LINKED)
#else
NODE_MODULE(ytnode, NYT::NNodeJS::ExportYT)
#endif
