#include "stdafx.h"
#include "profiling_manager.h"

#include <ytlib/misc/id_generator.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/fluent.h>

#include <util/datetime/cputimer.h>

namespace NYT {
namespace NProfiling  {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Profiling");

////////////////////////////////////////////////////////////////////////////////

struct TProfilingManager::TStoredSample
{
	i64 Id;
	TInstant Time;
	TValue Value;
};

////////////////////////////////////////////////////////////////////////////////

class TProfilingManager::TBucket
	: public TYPathServiceBase
	, public TSupportsGet
{
public:
	TBucket()
		: TYPathServiceBase("Profiling"/*Logger.GetCategory()*/)
	{ }

	void Store(const TStoredSample& sample)
	{
		// Samples are ordered by time.
		// Search for an appropriate insertion point starting from the back,
		// this should usually be fast.
		int index = static_cast<int>(Samples.size());
		while (index > 0 && Samples[index - 1].Time > sample.Time) {
			--index;
		}
		Samples.insert(Samples.begin() + index, sample);
	}

	void Trim(TDuration maxKeepInterval)
	{
		if (Samples.size() <= 1)
			return;

		auto deadline = Samples.back().Time - maxKeepInterval;
		while (Samples.front().Time < deadline) {
			Samples.pop_front();
		}
	}

private:
	std::deque<TStoredSample> Samples;

	void DoInvoke(NRpc::IServiceContext* context)
	{
		DISPATCH_YPATH_SERVICE_METHOD(Get);
		TYPathServiceBase::DoInvoke(context);
	}

	void GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
	{
		TYson yson = BuildYsonFluently()
			.DoListFor(Samples, [] (TFluentList fluent, const TStoredSample& sample)
				{
					fluent
						.Item().BeginMap()
							.Item("id").Scalar(sample.Id)
							.Item("time").Scalar(static_cast<i64>(sample.Time.MicroSeconds()))
							.Item("value").Scalar(sample.Value)
						.EndMap();
				});
		response->set_value(yson);
		context->Reply();
	}

};

////////////////////////////////////////////////////////////////////////////////

class TProfilingManager::TClockConverter
{
public:
	TClockConverter()
	{
		Calibrate();
	}

	TInstant ToInstant(TCpuClock clock)
	{
		CalibrateIfNeeded();
		return CalibrationInstant + CyclesToDuration(clock - CalibrationClock);
	}

private:
	static const TDuration CalibrationInterval;

	void CalibrateIfNeeded()
	{
		auto nowClock = GetCycleCount();
		if (nowClock > NextCalibrationClock) {
			Calibrate();
		}
	}

	void Calibrate()
	{
		CalibrationClock = GetCycleCount();
		CalibrationInstant = TInstant::Now();
		NextCalibrationClock = CalibrationClock + DurationToCycles(CalibrationInterval);
	}

	TInstant CalibrationInstant;
	TCpuClock CalibrationClock;
	TCpuClock NextCalibrationClock;

};

const TDuration TProfilingManager::TClockConverter::CalibrationInterval = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

class TProfilingManager::TImpl
	: public TActionQueueBase
{
public:
	TImpl()
		: TActionQueueBase("Profiling", true)
		, Invoker(New<TQueueInvoker>(this, true))
		, Root(GetEphemeralNodeFactory()->CreateMap())
		, Service(Root->Via(~Invoker))
	{
		Start();
	}

	~TImpl()
	{
		Invoker->OnShutdown();
	}

	void Enqueue(const TQueuedSample& sample)
	{
		SampleQueue.Enqueue(sample);
		Signal();
	}

	IInvoker* GetInvoker() const
	{
		return ~Invoker; 
	}

	IYPathService* GetService() const
	{
		return ~Service;
	}

private:
	TQueueInvokerPtr Invoker;
	INodePtr Root;
	IYPathServicePtr Service;
	TLockFreeQueue<TQueuedSample> SampleQueue;
	yhash_map<TYPath, TWeakPtr<TBucket> > PathToBucket;
	TIdGenerator<i64> IdGenerator;
	TClockConverter ClockConverter;

	static const TDuration MaxKeepInterval;

	bool DequeueAndExecute()
	{
		// Handle pending callbacks first.
		if (Invoker->OnDequeueAndExecute()) {
			return true;
		}

		// Process all pending samples in a row.
		bool samplesProcessed = false;
		TQueuedSample sample;
		while (SampleQueue.Dequeue(&sample)) {
			ProcessSample(sample);
			samplesProcessed = true;
		}

		return samplesProcessed;
	}

	TBucketPtr LookupBucket(const TYPath& path)
	{
		auto it = PathToBucket.find(path);
		if (it != PathToBucket.end()) {
			auto bucket = it->second.Lock();
			if (bucket) {
				return bucket;
			}
		}

		LOG_INFO("Creating new bucket %s", ~path.Quote());
		auto bucket = New<TBucket>();
		PathToBucket[path] = bucket;

		auto node = CreateVirtualNode(~bucket);
		SyncYPathSetNode(~Root, path, ~node);

		return bucket;
	}

	// TODO(babenko): currently not used
	void SweepBucketCache()
	{
		auto it = PathToBucket.begin();
		while (it != PathToBucket.end()) {
			auto jt = it;
			++jt;
			auto node = it->second.Lock();
			if (!node) {
				PathToBucket.erase(it);
			}
			it = jt;
		}
	}

	void ProcessSample(TQueuedSample& queuedSample)
	{
		auto path = CombineYPaths(queuedSample.PathPrefix, queuedSample.Path);

		auto bucket = LookupBucket(path);

		TStoredSample storedSample;
		storedSample.Id = IdGenerator.Next();
		storedSample.Time = ClockConverter.ToInstant(queuedSample.Time);
		storedSample.Value = queuedSample.Value;

		bucket->Store(storedSample);
		bucket->Trim(MaxKeepInterval);
	}

};

// TODO(babenko): make configurable?
const TDuration TProfilingManager::TImpl::MaxKeepInterval = TDuration::Seconds(60);

////////////////////////////////////////////////////////////////////////////////

TProfilingManager::TProfilingManager()
	: Impl(new TImpl())
{ }

TProfilingManager* TProfilingManager::Get()
{
    return Singleton<TProfilingManager>();
}

void TProfilingManager::Enqueue(const TQueuedSample& sample)
{
	Impl->Enqueue(sample);
}

IInvoker* TProfilingManager::GetInvoker() const
{
	return Impl->GetInvoker();
}

NYTree::IYPathService* TProfilingManager::GetService() const
{
	return Impl->GetService();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
