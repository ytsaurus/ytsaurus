#pragma once

#include "common.h"
#include "async_ref_counted.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSStreamBase
    : public TAsyncRefCountedObjectWrap
{
public:
    using TAsyncRefCountedObjectWrap::AsyncRef;
    using TAsyncRefCountedObjectWrap::AsyncUnref;

protected:
    TNodeJSStreamBase();
    ~TNodeJSStreamBase();

    const ui32 Id_ = RandomNumber<ui32>();

    struct TInputPart
    {
        v8::Persistent<v8::Value> Handle;

        // The following data is owned by the handle hence no need to care
        // about freeing memory after structure disposal.
        char*  Buffer;
        size_t Offset;
        size_t Length;
    };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
