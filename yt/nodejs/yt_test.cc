#include <node.h>
#include <node_buffer.h>
#include <v8.h>
#include <uv.h>

#include <pthread.h>

#include <deque>
#include <tr1/memory>

////////////////////////////////////////////////////////////////////////////////

#define TRACE_CURRENT_THREAD() (fprintf(stderr, "=== Thread 0x%012lx: %s\n", (size_t)pthread_self(), __PRETTY_FUNCTION__))

#define CHECK_RETURN_VALUE(expr) do { int rv = (expr); assert(rv == 0 && #expr); } while (0)

#define INPUT_QUEUE_SIZE 128

using v8::Arguments;
using v8::Local;
using v8::Persistent;
using v8::Handle;
using v8::HandleScope;
using v8::Value;
using v8::String;
using v8::Object;
using v8::FunctionTemplate;
using v8::Undefined;
using v8::Null;

class TGuard {
public:
    TGuard(pthread_mutex_t* mutex, bool acquire = true)
        : Mutex(mutex)
    {
        if (acquire) {
            pthread_mutex_lock(Mutex);
        }
    }

    ~TGuard()
    {
        pthread_mutex_unlock(Mutex);
    }

private:
    pthread_mutex_t* Mutex;
};

class TInputStream {
    virtual ~TInputStream();

    size_t Read(void* buf, size_t len);
    virtual size_t DoRead(void* buf, size_t len);
    virtual size_t DoSkip(size_t len);
};

class TOutputStream {
    virtual ~TOutputStream();

    virtual void DoWrite(const void* buf, size_t len);
//    virtual void DoWriteV(const TPart* parts, size_t count);
    virtual void DoFlush();
    virtual void DoFinish();
};

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static void DoNothing(uv_work_t*)
{ }

////////////////////////////////////////////////////////////////////////////////

class TNodeJSStreamBase;

//! This class adheres to TInputStream interface as a C++ object and
//! simultaneously provides 'writable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from JS to C++.
class TNodeJSInputStream;

//! This class adheres to TOutputStream interface as a C++ object and
//! simultaneously provides 'readable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from C++ to JS.
class TNodeJSOutputStream;

//! A NodeJS driver host.
class TNodeJSDriverHost;

////////////////////////////////////////////////////////////////////////////////

class TNodeJSStreamBase
{
public:
    TNodeJSStreamBase();
    virtual ~TNodeJSStreamBase();

    struct TPart
    {
        Persistent<Value> Buffer;

        char*  Data;
        size_t Offset;
        size_t Length;

        TPart()
            : Buffer()
            , Data(NULL)
            , Offset(-1)
            , Length(-1)
        { }
    };

    typedef std::deque<TPart> TQueue;

private:
    TNodeJSStreamBase(const TNodeJSStreamBase&);
    TNodeJSStreamBase& operator=(const TNodeJSStreamBase&);
};

TNodeJSStreamBase::TNodeJSStreamBase()
{ }

TNodeJSStreamBase::~TNodeJSStreamBase()
{ }

////////////////////////////////////////////////////////////////////////////////

class TNodeJSInputStream
    : public TNodeJSStreamBase
    //: private TNonCopyable
{
public:
    TNodeJSInputStream();
    ~TNodeJSInputStream();

public:
    // Synchronous JS API.
    void Push(Handle<Value> buffer, char *data, size_t offset, size_t length);

    // Asynchronous JS API.
    void AsyncSweep();
    static void Sweep(uv_work_t *request);
    void DoSweep();

    void AsyncClose();
    static void Close(uv_work_t *request);
    void DoClose();

    // C++ API.
    size_t Read(void* buffer, size_t length);

private:
    friend class TNodeJSDriverHost;
    
    bool IsAlive;
    pthread_mutex_t Mutex;
    pthread_cond_t Conditional;
    TQueue Queue;

    uv_work_t SweepRequest;
    uv_work_t CloseRequest;
};

TNodeJSInputStream::TNodeJSInputStream()
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();

    CHECK_RETURN_VALUE(pthread_mutex_init(&Mutex, NULL));
    CHECK_RETURN_VALUE(pthread_cond_init(&Conditional, NULL));

    IsAlive = true;
}

TNodeJSInputStream::~TNodeJSInputStream()
{
    // Affinity: any?
    TRACE_CURRENT_THREAD();

    {
        TGuard guard(&Mutex);
        assert(Queue.empty());
    }

    CHECK_RETURN_VALUE(pthread_mutex_destroy(&Mutex));
    CHECK_RETURN_VALUE(pthread_cond_destroy(&Conditional));
}

void TNodeJSInputStream::Push(Handle<Value> buffer, char *data, size_t offset, size_t length)
{
    // Affinity: V8
    HandleScope scope;

    TPart part;
    part.Buffer = Persistent<Value>::New(buffer);
    part.Data   = data;
    part.Offset = offset;
    part.Length = length;

    fprintf(stderr, "(PUSH) buffer = %p, offset = %lu, length = %lu\n",
        part.Data, part.Offset, part.Length);

    {
        TGuard guard(&Mutex);
        Queue.push_back(part);
        pthread_cond_broadcast(&Conditional);
    }
}

inline void TNodeJSInputStream::AsyncSweep()
{
    // Post to V8 thread.
    uv_queue_work(
        uv_default_loop(), &SweepRequest,
        DoNothing, TNodeJSInputStream::Sweep);
}

void TNodeJSInputStream::Sweep(uv_work_t *request)
{
    // Affinity: V8
    TNodeJSInputStream* stream =
        container_of(request, TNodeJSInputStream, SweepRequest);
    stream->DoSweep();
}

void TNodeJSInputStream::DoSweep()
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    // Since this function is invoked from V8, we are trying to avoid
    // all blocking operations. For example, it is better to reschedule
    // the sweep, if the mutex is already acquired.
    {
        int rv  = pthread_mutex_trylock(&Mutex);
        if (rv != 0) {
            AsyncSweep();
            return;
        }
    }

    TGuard guard(&Mutex, false);

    TQueue::iterator
        it = Queue.begin(),
        jt = Queue.end();

    while (it != jt) {
        TPart& current = *it;

        if (current.Length > 0) {
            break;
        } else {
            current.Buffer.Dispose();
            current.Buffer.Clear();
            ++it;
        }
    }

    Queue.erase(Queue.begin(), it);
}

inline void TNodeJSInputStream::AsyncClose()
{
    // Post to any thread.
    uv_queue_work(
        uv_default_loop(), &CloseRequest,
        TNodeJSInputStream::Close, DoNothing);
}

void TNodeJSInputStream::Close(uv_work_t *request)
{
    // Affinity: any
    TNodeJSInputStream* stream =
        container_of(request, TNodeJSInputStream, CloseRequest);
    stream->DoClose();
}

void TNodeJSInputStream::DoClose()
{
    // Affinity: any
    TRACE_CURRENT_THREAD();

    TGuard guard(&Mutex, false);

    IsAlive = false;
    pthread_cond_broadcast(&Conditional);
}

size_t TNodeJSInputStream::Read(void* buffer, size_t length)
{
    // Affinity: Any thread.
    TRACE_CURRENT_THREAD();

    TGuard guard(&Mutex);

    size_t result = 0;
    while (length > 0 && result == 0) {
        TQueue::iterator
            it = Queue.begin(),
            jt = Queue.end();

        size_t canRead;
        bool canReadSomething = false;

        while (length > 0 && it != jt) {
            TPart& current = *it;

            canRead = std::min(length, current.Length);
            canReadSomething |= (canRead > 0);

            ::memcpy(
                (char*)buffer + result,
                current.Data + current.Offset,
                canRead);

            result += canRead;
            length -= canRead;

            current.Offset += canRead;
            current.Length -= canRead;

            assert(length == 0 || current.Length == 0);

            ++it;
        }

        if (!canReadSomething) {
            if (IsAlive) {
                CHECK_RETURN_VALUE(pthread_cond_wait(&Conditional, &Mutex));
                continue;
            } else {
                return 0;
            }
        }
    };

    AsyncSweep();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TNodeJSOutputStream
    : public TNodeJSStreamBase
{
public:
    TNodeJSOutputStream();
    ~TNodeJSOutputStream();

public:
    // Synchronous JS API.
    Handle<Value> Pull();

    // Asynchronous JS API.
    // C++ API.
    void Write(const void* buffer, size_t length);

private:
    friend class TNodeJSDriverHost;

    static void DeleteCallback(char* data, void* hint);

    pthread_mutex_t Mutex;
    TQueue Queue;
};

TNodeJSOutputStream::TNodeJSOutputStream()
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();

    CHECK_RETURN_VALUE(pthread_mutex_init(&Mutex, NULL));
}

TNodeJSOutputStream::~TNodeJSOutputStream()
{
    // Affinity: any?
    TRACE_CURRENT_THREAD();

    CHECK_RETURN_VALUE(pthread_mutex_destroy(&Mutex));

    // TODO(sandello): Use FOREACH.
    TQueue::iterator
        it = Queue.begin(),
        jt = Queue.end();

    while (it != jt) {
        if (it->Data) {
            delete[] (char*)it->Data;
            it->Data = NULL;
        }
    }
}

Handle<Value> TNodeJSOutputStream::Pull()
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    TGuard guard(&Mutex);

    if (Queue.empty()) {
        return Null();
    } else {
        TPart& front = Queue.front();

        node::Buffer* buffer =
            node::Buffer::New(front.Data, front.Length, DeleteCallback, 0);

        Queue.pop_front();
        return scope.Close(buffer->handle_);
    }
}

/*

TODO

- implement a test callback , which writes some data to output stream
- pull data from JS and test in that way

*/

void TNodeJSOutputStream::Write(const void *buffer, size_t length)
{
    // Affinity: any
    // TODO(sandello): Use some kind of holder here.
    char* data = new char[length]; assert(data);
    ::memcpy(data, buffer, length);

    TPart part;
    part.Data   = data;
    part.Offset = 0;
    part.Length = length;

    {
        TGuard guard(&Mutex);
        Queue.push_back(part);
    }
}

void TNodeJSOutputStream::DeleteCallback(char *data, void *hint)
{
    delete[] data;
}

////////////////////////////////////////////////////////////////////////////////

class TNodeJSDriverHost
    : public node::ObjectWrap
{
public:
    ~TNodeJSDriverHost();

    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    // Synchronous JS API.
    static Handle<Value> New(const Arguments& args);
    static Handle<Value> WriteToInputStream(const Arguments& args);
    static Handle<Value> ReadFromOutputStream(const Arguments& args);
    static Handle<Value> ReadFromErrorStream(const Arguments& args);
    static Handle<Value> CloseInputStream(const Arguments& args);
    static Handle<Value> CloseOutputStream(const Arguments& args);
    static Handle<Value> CloseErrorStream(const Arguments& args);

    // Asynchronous JS API.
    static Handle<Value> Test(const Arguments& args);
    static void TestWork(uv_work_t* workRequest);
    static void TestAfter(uv_work_t* workRequest);

private:
    std::tr1::shared_ptr<TNodeJSInputStream> InputStream;
//    std::list<> OutputStreamParts;
//    std::list<> ErrorStreamParts;

    uv_work_t TestRequest;

private:
    static TNodeJSDriverHost* Create();
    TNodeJSDriverHost();
    TNodeJSDriverHost(const TNodeJSDriverHost&);
};

Handle<Value> TNodeJSDriverHost::New(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    TNodeJSDriverHost* host = new TNodeJSDriverHost();
    host->Wrap(args.This());
    return args.This();
}

Handle<Value> TNodeJSDriverHost::WriteToInputStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    TNodeJSDriverHost* host =
        ObjectWrap::Unwrap<TNodeJSDriverHost>(args.This());

    // Validate arguments.
    assert(args.Length() == 3);

    assert(node::Buffer::HasInstance(args[0]));
    assert(args[1]->IsUint32());
    assert(args[2]->IsUint32());

    // Do the work.
    host->InputStream->Push(
        /* buffer */ args[0],
        /* data   */ node::Buffer::Data(Local<Object>::Cast(args[0])),
        /* offset */ args[1]->Uint32Value(),
        /* length */ args[2]->Uint32Value());

    // TODO(sandello): Think about OnSuccess & OnError callbacks.
    return Undefined();
}

Handle<Value> TNodeJSDriverHost::ReadFromOutputStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    return Undefined();
}

Handle<Value> TNodeJSDriverHost::ReadFromErrorStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    return Undefined();
}

Handle<Value> TNodeJSDriverHost::CloseInputStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    TNodeJSDriverHost* host =
        ObjectWrap::Unwrap<TNodeJSDriverHost>(args.This());

    host->InputStream->AsyncClose();
    return Undefined();
}

Handle<Value> TNodeJSDriverHost::CloseOutputStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    return Undefined();
}

Handle<Value> TNodeJSDriverHost::CloseErrorStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    return Undefined();
}

struct TTestRequest
{
    uv_work_t TestRequest;
    TNodeJSDriverHost* Host;
    size_t HowMuch;
    char* WhereToRead;
};

Handle<Value> TNodeJSDriverHost::Test(const Arguments &args)
{
    //TRACE_CURRENT_THREAD();
    assert(args.Length() == 1);
    assert(args[0]->IsUint32());

    TTestRequest* request = new TTestRequest();

    request->Host = ObjectWrap::Unwrap<TNodeJSDriverHost>(args.This());
    request->HowMuch = args[0]->ToUint32()->Value();
    request->WhereToRead = new char[request->HowMuch];

    uv_queue_work(
        uv_default_loop(), &request->TestRequest,
        TNodeJSDriverHost::TestWork, TNodeJSDriverHost::TestAfter);

    request->Host->Ref();
    return Undefined();
}

void TNodeJSDriverHost::TestWork(uv_work_t *workRequest)
{
    //TRACE_CURRENT_THREAD();
    TTestRequest* request = container_of(workRequest, TTestRequest, TestRequest);

    size_t x = request->Host->InputStream->Read(request->WhereToRead, request->HowMuch);
    request->HowMuch = x;
}

void TNodeJSDriverHost::TestAfter(uv_work_t *workRequest)
{
    //TRACE_CURRENT_THREAD();
    TTestRequest* request = container_of(workRequest, TTestRequest, TestRequest);

    fprintf(stderr, ">>>READ>>>");
    fwrite(request->WhereToRead, 1, request->HowMuch, stderr);
    fprintf(stderr, "<<< (len=%lu)\n", request->HowMuch);

    request->Host->Unref();

    delete[] request->WhereToRead;
    delete request;
}

TNodeJSDriverHost* TNodeJSDriverHost::Create()
{
    TRACE_CURRENT_THREAD();
    return new TNodeJSDriverHost();
}

TNodeJSDriverHost::TNodeJSDriverHost()
    : node::ObjectWrap()
    , InputStream(new TNodeJSInputStream())
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
}

TNodeJSDriverHost::~TNodeJSDriverHost()
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
}

////////////////////////////////////////////////////////////////////////////////

void InitYT(Handle<Object> target)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(TNodeJSDriverHost::New);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    NODE_SET_PROTOTYPE_METHOD(tpl, "write_to_input", TNodeJSDriverHost::WriteToInputStream);
    NODE_SET_PROTOTYPE_METHOD(tpl, "read_from_output", TNodeJSDriverHost::ReadFromOutputStream);
    NODE_SET_PROTOTYPE_METHOD(tpl, "read_from_error", TNodeJSDriverHost::ReadFromErrorStream);
    NODE_SET_PROTOTYPE_METHOD(tpl, "close_input", TNodeJSDriverHost::CloseInputStream);
    NODE_SET_PROTOTYPE_METHOD(tpl, "test", TNodeJSDriverHost::Test);

    tpl->SetClassName(String::NewSymbol("DriverHost"));
    target->Set(String::NewSymbol("DriverHost"), tpl->GetFunction());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

NODE_MODULE(yt_test, NYT::InitYT)
