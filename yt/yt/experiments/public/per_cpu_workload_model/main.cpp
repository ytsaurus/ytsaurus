#include <yt/yt/server/lib/io/io_workload_model.h>

#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/profiling/tscp.h>

#include <vector>
#include <thread>

using namespace NYT;
using namespace NYT::NIO;
using namespace NYT::NProfiling;

struct TStatsHolder
{
    TRequestSizes RequestSizes_;
    TRequestLatencies RequestLatencies_;

    void Register(i64 requestSize, EWorkloadCategory category, TDuration requestTime)
    {
        auto guard = Guard(StatsLock_);

        RequestSizes_.Reads[category].RecordValue(requestSize);
        RequestLatencies_.Reads[category].RecordValue(requestTime.MilliSeconds());
    }

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StatsLock_);
};

struct TPerCpuStatsHolder
{
    void Register(i64 requestSize, EWorkloadCategory category, TDuration requestTime)
    {
        auto tscp = TTscp::Get();
        auto& holder = Shards_[tscp.ProcessorId].Holder;
        holder.Register(requestSize, category, requestTime);
    }

private:
    struct alignas(2 * CacheLineSize) TShard
    {
        TStatsHolder Holder;
    };

    std::array<TShard, TTscp::MaxProcessorId> Shards_;
};

void ExecTimes(i64 count, auto& holder)
{
    for (i64 index = 0; index < count; ++index) {
        holder.Register(index, static_cast<EWorkloadCategory>(index % 7), TDuration::MilliSeconds(index));
    }
}

int main()
{
    // TStatsHolder holder;
    TPerCpuStatsHolder holder;
    constexpr int ThreadsCount = 1024;

    std::vector<std::thread> threads;
    threads.reserve(ThreadsCount);
    for (int i = 0; i < ThreadsCount; ++i) {
        threads.push_back(std::thread([&] () {
            // TStatsHolder holder;
            ExecTimes(100_MB / ThreadsCount, holder);
        }));
    }

    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
