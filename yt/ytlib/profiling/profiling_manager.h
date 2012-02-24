#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Represents a sample that is enqueued to a bucket.
struct TQueuedSample
{
    TQueuedSample()
		: Time(-1)
		, Value(-1)
	{ }

    ui64 Time;
	NYTree::TYPath Path;
    TValue Value;
};

////////////////////////////////////////////////////////////////////////////////

//! A backend that controls all profiling activities.
/*!
 *  This class is a singleton, call #Get to obtain an instance.
 *  
 *  Processing happens in the background thread that maintains
 *  a queue of all incoming (queued) samples and distributes them into buckets.
 *  
 *  This thread also provides a invoker for executing various callbacks.
 */
class TProfilingManager
{
public:
    TProfilingManager();

	//! Returns the singleton instance.
    static TProfilingManager* Get();

	//! Enqueues a new sample for processing.
    void Enqueue(const TQueuedSample& sample);

	//! Returns the invoker associated with the profiler thread.
	IInvoker* GetInvoker() const;

	//! Returns the root of the YTree with the buckets.
	/*!
	 *  \note
	 *  The latter must only be accessed from the invoker returned by #GetInvoker.
	 */
	NYTree::IMapNode* GetRoot() const;

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
