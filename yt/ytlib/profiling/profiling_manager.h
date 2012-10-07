#pragma once

#include "public.h"

#include <ytlib/actions/invoker.h>
#include <ytlib/ytree/public.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Represents a sample that was enqueued to the profiler but did not
//! reach the storage yet.
struct TQueuedSample
{
    TQueuedSample()
        : Time(-1)
        , Value(-1)
    { }

    TCpuInstant Time;
    NYPath::TYPath Path;
    TValue Value;
};

////////////////////////////////////////////////////////////////////////////////

//! A backend that controls all profiling activities.
/*!
 *  This class is a singleton, call #Get to obtain an instance.
 *  
 *  Processing happens in a background thread that maintains
 *  a queue of all incoming (queued) samples and distributes them into buckets.
 *  
 *  This thread also provides a invoker for executing various callbacks.
 */
class TProfilingManager
    : private TNonCopyable
{
public:
    TProfilingManager();

    //! Returns the singleton instance.
    static TProfilingManager* Get();

    //! Starts profiling.
    /*!
     *  No samples are collected before this method is called.
     */
    void Start();

    //! Shuts down the profiling system.
    /*!
     *  After this call #Enqueue has no effect.
     */
    void Shutdown();

    //! Enqueues a new sample for processing.
    void Enqueue(const TQueuedSample& sample, bool selfProfiling);

    //! Returns the invoker associated with the profiler thread.
    IInvokerPtr GetInvoker() const;

    //! Returns the root of the YTree with the buckets.
    /*!
     *  The latter must only be accessed from the invoker returned by #GetInvoker.
     */
    NYTree::IMapNodePtr GetRoot() const;

private:
    class TClockConverter;
    class TImpl;

    class TBucket;
    typedef TIntrusivePtr<TBucket> TBucketPtr;

    struct TStoredSample;

    // Cannot use THolder here since TImpl inherits from TActionQueueBase
    // and is thus ref-counted.
    TIntrusivePtr<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT

template <>
struct TSingletonTraits<NYT::NProfiling::TProfilingManager>
{
    enum
    {
        Priority = 2048
    };
};
