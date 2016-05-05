#pragma once

#include "common.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSStreamBase
    : public node::ObjectWrap
{
protected:
    TNodeJSStreamBase();
    ~TNodeJSStreamBase();

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    void AsyncRef(bool acquireSyncRef);
    void AsyncUnref();

protected:
    const ui32 Id_ = RandomNumber<ui32>();
    std::atomic<int> AsyncRefCounter_ = {0};

protected:
    struct TOutputPart
    {
        TOutputPart() = delete;
        TOutputPart(TOutputPart&) = delete;
        TOutputPart(TOutputPart&&) = default;

        TOutputPart(std::unique_ptr<char[]> buffer, size_t length)
            : Buffer(std::move(buffer))
            , Length(length)
        { }

        std::unique_ptr<char[]> Buffer = nullptr;
        size_t Length = 0;

        inline explicit operator bool() const
        {
            return Buffer != nullptr && Length > 0;
        }
    };

    struct TInputPart
    {
        v8::Persistent<v8::Value> Handle;

        // The following data is owned by the handle hence no need to care
        // about freeing memory after structure disposal.
        char*  Buffer;
        size_t Offset;
        size_t Length;
    };

    template <bool acquireSyncRef>
    class TScopedRef
    {
        TNodeJSStreamBase* Stream_;
    public:
        explicit TScopedRef(TNodeJSStreamBase* stream)
            : Stream_(stream)
        {
            Stream_->AsyncRef(acquireSyncRef);
        }
        ~TScopedRef()
        {
            Stream_->AsyncUnref();
        }
    };

private:
    TNodeJSStreamBase(const TNodeJSStreamBase&) = delete;
    TNodeJSStreamBase(TNodeJSStreamBase&&) = delete;
    TNodeJSStreamBase& operator=(const TNodeJSStreamBase&) = delete;
    TNodeJSStreamBase& operator=(TNodeJSStreamBase&&) = delete;

    static int UnrefCallback(eio_req*);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
