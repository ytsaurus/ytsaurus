#include "input_manager.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/misc/atomic_ptr.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/memory/new.h>

namespace NYT::NFlow::NWorker {

using namespace NThreading;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TJobMap final
{
    static constexpr bool EnableHazard = true;

    THashMap<TJobId, IInputBufferPtr> JobIdToInputBuffer;
};

////////////////////////////////////////////////////////////////////////////////

class TInputManager
    : public IInputManager
{
public:
    TInputManager()
        : JobMap_(New<TJobMap>())
    { }

    void AddJob(TJobId jobId, IInputBufferPtr inputBuffer) override
    {
        auto guard = Guard(WriterLock_);
        auto current = JobMap_.AcquireHazard();
        auto next = New<TJobMap>(*current);
        EmplaceOrCrash(next->JobIdToInputBuffer, jobId, std::move(inputBuffer));
        JobMap_.Store(std::move(next));
    }

    void RemoveJob(TJobId jobId) override
    {
        auto guard = Guard(WriterLock_);
        auto current = JobMap_.AcquireHazard();
        auto next = New<TJobMap>(*current);
        EraseOrCrash(next->JobIdToInputBuffer, jobId);
        JobMap_.Store(std::move(next));
    }

    IInputBufferPtr GetInputBuffer(TJobId jobId) const override
    {
        auto snapshot = JobMap_.AcquireHazard();
        return GetOrDefault(snapshot->JobIdToInputBuffer, jobId, nullptr);
    }

private:
    YT_DECLARE_SPIN_LOCK(TSpinLock, WriterLock_);
    TAtomicPtr<TJobMap, /*EnableAcquireHazard*/ true> JobMap_;
};

////////////////////////////////////////////////////////////////////////////////

IInputManagerPtr CreateInputManager()
{
    return New<TInputManager>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
