#include "linuxaio.h"

#include <yt/yt/core/misc/error.h>

#include <util/system/error.h>

#include <linux/aio_abi.h>
#include <unistd.h>
#include <sys/syscall.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

inline static int io_setup(unsigned nr, aio_context_t *ctxp)
{
    return syscall(__NR_io_setup, nr, ctxp);
}

inline static int io_destroy(aio_context_t ctx)
{
    return syscall(__NR_io_destroy, ctx);
}

inline static int io_submit(aio_context_t ctx, long nr, struct iocb*const* iocbpp)
{
    return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

inline static int io_getevents(aio_context_t ctx, long min_nr, long max_nr,
                   struct io_event *events,
                   struct timespec *timeout)
{
    return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

#define AIO_RING_MAGIC    0xa10a10a1
#define read_barrier()     __asm__ __volatile__("":::"memory")

struct aio_ring {
    unsigned id; /** kernel internal index number */
    unsigned nr; /** number of io_events */
    volatile unsigned head;
    volatile unsigned tail;

    unsigned magic;
    unsigned compat_features;
    unsigned incompat_features;
    unsigned header_length; /** size of aio_ring */

    struct io_event events[0];
};

int io_getevents_user(aio_context_t ctx, long nr, struct io_event *events)
{
    long i = 0;
    unsigned head;
    struct aio_ring *ring = (struct aio_ring*) ctx;

    if (ring->magic == AIO_RING_MAGIC) {
        while (i < nr) {
            head = ring->head;
            if (head == ring->tail) {
                break;
            } else {
                events[i] = ring->events[head];
                read_barrier();
                ring->head = head + 1 ? head + 1 < ring->nr : 0;
                ++i;
            }
        }
    }

    return i;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TLinuxAioContext LinuxAioCreate(unsigned queueSize)
{
    TLinuxAioContext context = {};
    int r = NDetail::io_setup(queueSize, &context);
    if (r != 0) {
        THROW_ERROR_EXCEPTION("Unable to create Linux AIO context: %Qv",
            LastSystemErrorText());
    }
    return context;
}

void LinuxAioDestroy(TLinuxAioContext context)
{
    int r = NDetail::io_destroy(context);
    if (r != 0) {
        THROW_ERROR_EXCEPTION("Unable to destroy Linux AIO context: %Qv",
            LastSystemErrorText());
    }
}

int LinuxAioSubmit(TLinuxAioContext context, const std::vector<TLinuxAioControlBlock*> controlBlocks)
{
    int r = NDetail::io_submit(context, controlBlocks.size(), &controlBlocks[0]);
    if (r < 0) {
        THROW_ERROR_EXCEPTION("Unable to submit to Linux AIO: %Qv",
            LastSystemErrorText());
    }
    return r;
}

int LinuxAioGetEvents(TLinuxAioContext context, int minEvents, TDuration timeout, std::vector<TLinuxAioEvent>* events)
{
    struct timespec timespecTimeout;
    timespecTimeout.tv_sec = timeout.Seconds();
    timespecTimeout.tv_nsec = (timeout.MicroSeconds() % 1000000) * 1000;

    auto* timespecp = timeout != TDuration() ? &timespecTimeout : nullptr;
    int r = NDetail::io_getevents(context, minEvents, events->size(), &events->front(), timespecp);
    if (r < 0) {
        THROW_ERROR_EXCEPTION("Unable to wait for Linux AIO events: %Qv",
            LastSystemErrorText());
    }
    return r;
}

int LinuxAioGetEventsInUserspace(TLinuxAioContext context, std::vector<TLinuxAioEvent>* events)
{
    return NDetail::io_getevents_user(context, events->size(), &events->front());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
