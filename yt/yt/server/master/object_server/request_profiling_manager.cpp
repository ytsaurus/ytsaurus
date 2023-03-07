#include "request_profiling_manager.h"

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NObjectServer {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TRequestProfilingCounters::TRequestProfilingCounters(const TTagIdList& tagIds)
    : TotalReadRequestCounter("/total_read_request_count", tagIds)
    , TotalWriteRequestCounter("/total_write_request_count", tagIds)
    , LocalReadRequestCounter("/local_read_request_count", tagIds)
    , LocalWriteRequestCounter("/local_write_request_count", tagIds)
    , LeaderFallbackRequestCounter("/leader_fallback_request_count", tagIds)
    , IntraCellForwardingRequestCounter("/intra_cell_forwarding_request_count", tagIds)
    , CrossCellForwardingRequestCounter("/cross_cell_forwarding_request_count", tagIds)
    , AutomatonForwardingRequestCounter("/automaton_forwarding_request_count", tagIds)
    , LocalMutationScheduleTimeCounter("/local_mutation_schedule_time", tagIds)
{ }

////////////////////////////////////////////////////////////////////////////////

class TRequestProfilingManager::TImpl
{
public:
    TRequestProfilingCountersPtr GetCounters(const TString& user, const TString& method)
    {
        auto key = std::make_tuple(user, method);

        {
            NConcurrency::TReaderGuard guard(Lock_);
            if (auto it = KeyToCounters_.find(key)) {
                return it->second;
            }
        }

        TTagIdList tagIds{
            TProfileManager::Get()->RegisterTag("user", user),
            TProfileManager::Get()->RegisterTag("method", method)
        };
        auto counters = New<TRequestProfilingCounters>(tagIds);

        {
            NConcurrency::TWriterGuard guard(Lock_);
            auto [it, inserted] = KeyToCounters_.emplace(key, std::move(counters));
            return it->second;
        }
    }

private:
    // (user, method)
    using TKey = std::tuple<TString, TString>;
    NConcurrency::TReaderWriterSpinLock Lock_;
    THashMap<TKey, TRequestProfilingCountersPtr> KeyToCounters_;
};

////////////////////////////////////////////////////////////////////////////////

TRequestProfilingManager::TRequestProfilingManager()
    : Impl_(std::make_unique<TImpl>())
{ }

TRequestProfilingManager::~TRequestProfilingManager()
{ }

TRequestProfilingCountersPtr TRequestProfilingManager::GetCounters(
    const TString& user,
    const TString& method)
{
    return Impl_->GetCounters(user, method);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
