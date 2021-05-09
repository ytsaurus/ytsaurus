#include "fair_share_action_queue.h"

#include "fair_share_queue_scheduler_thread.h"
#include "profiling_helpers.h"

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TFairShareActionQueue
    : public IFairShareActionQueue
{
public:
    TFairShareActionQueue(
        const TString& threadName,
        const std::vector<TString>& queueNames,
        const THashMap<TString, std::vector<TString>>& queueToBucket)
    {
        std::vector<TBucketDescription> bucketDescriptions;
        THashSet<TString> createdQueues;
        int nextBucketIndex = 0;
        for (const auto& [bucketName, bucketQueues] : queueToBucket) {
            auto& bucketDescription = bucketDescriptions.emplace_back();
            bucketDescription.BucketTagSet = GetBucketTags(threadName, bucketName);
            for (int queueIndex = 0; queueIndex < std::ssize(bucketQueues); ++queueIndex) {
                const auto& queueName = bucketQueues[queueIndex];
                YT_VERIFY(createdQueues.insert(queueName).second);
                QueueIndexToBucketIndex_.push_back(nextBucketIndex);
                QueueIndexToBucketQueueIndex_.push_back(queueIndex);
                bucketDescription.QueueTagSets.push_back(GetQueueTags(threadName, queueName));
            }
            ++nextBucketIndex;
        }

        // Create separate buckets for queues with no bucket specified.
        for (const auto& queueName : queueNames) {
            if (createdQueues.contains(queueName)) {
                continue;
            }

            auto& bucketDescription = bucketDescriptions.emplace_back();
            bucketDescription.BucketTagSet = GetBucketTags(threadName, queueName);
            bucketDescription.QueueTagSets.push_back(GetQueueTags(threadName, queueName));
            QueueIndexToBucketIndex_.push_back(nextBucketIndex++);
            QueueIndexToBucketQueueIndex_.push_back(0);
            YT_VERIFY(createdQueues.emplace(queueName).second);
        }

        YT_VERIFY(createdQueues.size() == queueNames.size());

        Queue_ = New<TFairShareInvokerQueue>(CallbackEventCount_, std::move(bucketDescriptions));
        Thread_ = New<TFairShareQueueSchedulerThread>(Queue_, CallbackEventCount_, threadName);
    }

    ~TFairShareActionQueue()
    {
        Shutdown();
    }

    virtual void Shutdown() override
    {
        bool expected = false;
        if (!ShutdownFlag_.compare_exchange_strong(expected, true)) {
            return;
        }

        Queue_->Shutdown();

        FinalizerInvoker_->Invoke(BIND([thread = Thread_, queue = Queue_] {
            thread->Shutdown();
            queue->Drain();
        }));
        FinalizerInvoker_.Reset();
    }

    virtual const IInvokerPtr& GetInvoker(int index) const override
    {
        YT_ASSERT(0 <= index && index < static_cast<int>(QueueIndexToBucketIndex_.size()));

        EnsuredStarted();
        return Queue_->GetInvoker(
            QueueIndexToBucketIndex_[index],
            QueueIndexToBucketQueueIndex_[index]);
    }

private:
    TFairShareInvokerQueuePtr Queue_;
    TFairShareQueueSchedulerThreadPtr Thread_;

    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();

    std::vector<int> QueueIndexToBucketIndex_;
    std::vector<int> QueueIndexToBucketQueueIndex_;

    mutable std::atomic<bool> StartFlag_ = false;
    std::atomic<bool> ShutdownFlag_ = false;

    IInvokerPtr FinalizerInvoker_ = GetFinalizerInvoker();

    void EnsuredStarted() const
    {
        bool expected = false;
        if (!StartFlag_.compare_exchange_strong(expected, true)) {
            return;
        }

        Thread_->Start();
    }
};

////////////////////////////////////////////////////////////////////////////////

IFairShareActionQueuePtr CreateFairShareActionQueue(
    const TString& threadName,
    const std::vector<TString>& queueNames,
    const THashMap<TString, std::vector<TString>>& queueToBucket)
{
    return New<TFairShareActionQueue>(threadName, queueNames, queueToBucket);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
