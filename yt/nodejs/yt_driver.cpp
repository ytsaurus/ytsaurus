#include "common.h"
#include "input_stream.h"
#include "output_stream.h"

#ifndef _GLIBCXX_PURE
#define _GLIBCXX_PURE inline
#endif

#include <string>

namespace NYT {

COMMON_V8_USES

using v8::Context;
using v8::Exception;
using v8::ThrowException;
using v8::TryCatch;

////////////////////////////////////////////////////////////////////////////////

// XXX(sandello): This is temporary; awaiting merge of 
// "babenko/new_driver" into mainline.

//! An instance of driver request.
struct TDriverRequest
{
    //! Command name to execute.
    std::string CommandName;

    //! Stream used for reading command input.
    TNodeJSInputStream* InputStream;

    //! Format used for reading the input.
    // TFormat InputFormat;

    //! Stream where the command output is written.
    TNodeJSOutputStream* OutputStream;

    //! Format used for writing the output.
    // TFormat OutputFormat;

    //! A map containing command arguments.
    // NYTree::IMapNodePtr Arguments;
};

//! An instance of driver request.
struct TDriverResponse
{
    //! An error returned by the command, if any.
    int Error;
};

struct IDriver
{
    TDriverResponse Execute(const TDriverRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

class TNodeJSDriver
    : public node::ObjectWrap
{
protected:
    TNodeJSDriver();
    ~TNodeJSDriver();

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    // Synchronous JS API.
    static Handle<Value> New(const Arguments& args);

    // Asynchronous JS API.
    static Handle<Value> Execute(const Arguments& args);
    static void ExecuteWork(uv_work_t* workRequest);
    static void ExecuteAfter(uv_work_t* workRequest);

private:
    // TODO(sandello): Store by smart pointer.
    IDriver* Driver;

    struct TExecuteRequest
    {
        uv_work_t Request;
        TNodeJSDriver* Host;

        Persistent<Function> Callback;

        TDriverRequest DriverRequest;
        TDriverResponse DriverResponse;

        TExecuteRequest(TNodeJSDriver* host, Handle<Function> callback)
            : Host(host)
            , Callback(Persistent<Function>::New(callback))
        {
            THREAD_AFFINITY_IS_V8();

            Host->Ref();
            // TODO(sandello): Ref streams here also.
        }

        ~TExecuteRequest()
        {
            THREAD_AFFINITY_IS_V8();

            Callback.Dispose();
            Callback.Clear();

            // TODO(sandello): Unref streams here also.
            Host->Unref();
        }
    };

private:
    TNodeJSDriver(const TNodeJSDriver&);
    TNodeJSDriver& operator=(const TNodeJSDriver&);
};

////////////////////////////////////////////////////////////////////////////////

TNodeJSDriver::TNodeJSDriver()
    : node::ObjectWrap()
{
    T_THREAD_AFFINITY_IS_V8();
}

TNodeJSDriver::~TNodeJSDriver()
{
    T_THREAD_AFFINITY_IS_V8();
}

Handle<Value> TNodeJSDriver::New(const Arguments& args)
{
    T_THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    assert(args.Length() == 0);

    TNodeJSDriver* host = new TNodeJSDriver();
    host->Wrap(args.This());
    return args.This();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSDriver::Execute(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    // These arguments are used to fill TDriverRequest structure,
    // hence we have to validate all the arguments as early as possible.
    assert(args.Length() == 7);

    EXPECT_THAT_IS(args[0], String); // CommandName
    EXPECT_THAT_HAS_INSTANCE(args[1], TNodeJSInputStream); // InputStream
    EXPECT_THAT_IS(args[2], String); // InputFormat
    EXPECT_THAT_HAS_INSTANCE(args[3], TNodeJSOutputStream); // OutputStream
    EXPECT_THAT_IS(args[4], String); // OutputFormat
    EXPECT_THAT_IS(args[5], Object); // Parameters
    EXPECT_THAT_IS(args[6], Function); // Callback

    // Unwrap arguments.
    String::AsciiValue commandName(args[0]);
    TNodeJSInputStream* inputStream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args[1]->ToObject());
    String::AsciiValue inputFormat(args[2]);
    TNodeJSOutputStream* outputStream =
        ObjectWrap::Unwrap<TNodeJSOutputStream>(args[3]->ToObject());
    String::AsciiValue outputFormat(args[4]);
    Local<Object> parameters = args[5]->ToObject();
    Local<Function> callback = Local<Function>::Cast(args[6]);

    // Build an atom of work.
    TExecuteRequest* request = new TExecuteRequest(
        ObjectWrap::Unwrap<TNodeJSDriver>(args.This()),
        callback);

    // Fill in TDriverRequest structure.
    request->DriverRequest.CommandName = std::string(*commandName, commandName.length());
    request->DriverRequest.InputStream = inputStream;
    // request->DriverRequest.InputFormat =
    request->DriverRequest.OutputStream = outputStream;
    // request->DriverRequest.OutputFormat =
    // request->DriverRequest.Parameters =

    uv_queue_work(
        uv_default_loop(), &request->Request,
        TNodeJSDriver::ExecuteWork, TNodeJSDriver::ExecuteAfter);

    return Undefined();
}

void TNodeJSDriver::ExecuteWork(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_UV();
    TExecuteRequest* request =
        container_of(workRequest, TExecuteRequest, Request);

    request->DriverResponse = request->Host->Driver->Execute(request->DriverRequest);
}

void TNodeJSDriver::ExecuteAfter(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TExecuteRequest* request =
        container_of(workRequest, TExecuteRequest, Request);

    {
        TryCatch block;

        Local<Value> args[] = {
            // TODO(sandello): Fix me.
            Integer::New(request->DriverResponse.Error)
        };

        request->Callback->Call(
            Context::GetCurrent()->Global(),
            ARRAY_SIZE(args),
            args);

        if (block.HasCaught()) {
            node::FatalException(block);
        }
    }

    delete request;
}

////////////////////////////////////////////////////////////////////////////////

void ExportYTDriver(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(TNodeJSDriver::New);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Execute", TNodeJSDriver::Execute);

    tpl->SetClassName(String::NewSymbol("TNodeJSDriver"));
    target->Set(String::NewSymbol("TNodeJSDriver"), tpl->GetFunction());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

NODE_MODULE(yt_driver, NYT::ExportYTDriver)
