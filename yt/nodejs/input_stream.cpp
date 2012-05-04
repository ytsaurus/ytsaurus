#include "input_stream.h"

#include <node.h>
#include <node_buffer.h>

namespace NYT {

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

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

} // namespace NYT
