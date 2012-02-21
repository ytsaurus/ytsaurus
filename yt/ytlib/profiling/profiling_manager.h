#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TQueuedSample
{
    TQueuedSample()
		: Time(-1)
		, Value(-1)
	{ }

    TCpuClock Time;
	// Full path is computed in the profiler thread.
	NYTree::TYPath PathPrefix;
	NYTree::TYPath Path;
    TValue Value;
};

////////////////////////////////////////////////////////////////////////////////

class TProfilingManager
{
public:
    TProfilingManager();

    static TProfilingManager* Get();

    void Enqueue(const TQueuedSample& sample);

	IInvoker* GetInvoker() const;
	NYTree::IYPathService* GetService() const;

private:
	class TTimeConverter;
	class TImpl;

	class TBucket;
	typedef TIntrusivePtr<TBucket> TBucketPtr;

	struct TStoredSample;

	THolder<TImpl> Impl;
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
