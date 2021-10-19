#include "fair_share_action_queue.h"

#include "fair_share_queue_scheduler_thread.h"
#include "profiling_helpers.h"

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/misc/collection_helpers.h>

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
        THashMap<TString, int> queueNameToIndex;
        for (int queueIndex = 0; queueIndex < std::ssize(queueNames); ++queueIndex) {
            YT_VERIFY(queueNameToIndex.emplace(queueNames[queueIndex], queueIndex).second);
        }

        QueueIndexToBucketIndex_.resize(queueNames.size(), -1);
        QueueIndexToBucketQueueIndex_.resize(queueNames.size(), -1);
        BucketNames_.reserve(queueNames.size());

        std::vector<TBucketDescription> bucketDescriptions;
        THashSet<TString> createdQueues;
        int nextBucketIndex = 0;
        for (const auto& [bucketName, bucketQueues] : queueToBucket) {
            int bucketIndex = nextBucketIndex++;
            auto& bucketDescription = bucketDescriptions.emplace_back();
            bucketDescription.BucketTagSet = GetBucketTags(threadName, bucketName);
            for (int bucketQueueIndex = 0; bucketQueueIndex < std::ssize(bucketQueues); ++bucketQueueIndex) {
                const auto& queueName = bucketQueues[bucketQueueIndex];
                auto queueIndex = GetOrCrash(queueNameToIndex, queueName);
                YT_VERIFY(createdQueues.insert(queueName).second);
                QueueIndexToBucketIndex_[queueIndex] = bucketIndex;
                QueueIndexToBucketQueueIndex_[queueIndex] = bucketQueueIndex;
                bucketDescription.QueueTagSets.push_back(GetQueueTags(threadName, queueName));
            }
            BucketNames_.push_back(bucketName);
        }

        // Create separate buckets for queues with no bucket specified.
        for (const auto& queueName : queueNames) {
            if (createdQueues.contains(queueName)) {
                continue;
            }

            auto& bucketDescription = bucketDescriptions.emplace_back();
            bucketDescription.BucketTagSet = GetBucketTags(threadName, queueName);
            bucketDescription.QueueTagSets.push_back(GetQueueTags(threadName, queueName));
            auto queueIndex = GetOrCrash(queueNameToIndex, queueName);
            QueueIndexToBucketIndex_[queueIndex] = nextBucketIndex++;
            QueueIndexToBucketQueueIndex_[queueIndex] = 0;
            YT_VERIFY(createdQueues.emplace(queueName).second);
            BucketNames_.push_back(queueName);
        }

        YT_VERIFY(createdQueues.size() == queueNames.size());
        YT_VERIFY(BucketNames_.size() == bucketDescriptions.size());

        for (int queueIndex = 0; queueIndex < std::ssize(queueNames); ++queueIndex) {
            YT_VERIFY(QueueIndexToBucketIndex_[queueIndex] != -1);
            YT_VERIFY(QueueIndexToBucketQueueIndex_[queueIndex] != -1);
        }

        Queue_ = New<TFairShareInvokerQueue>(CallbackEventCount_, std::move(bucketDescriptions));
        Thread_ = New<TFairShareQueueSchedulerThread>(Queue_, CallbackEventCount_, threadName, threadName);
    }

    ~TFairShareActionQueue()
    {
        Shutdown();
    }

    void Shutdown()
    {
        if (Stopped_.exchange(true)) {
            return;
        }

        Queue_->Shutdown();

        FinalizerInvoker_->Invoke(BIND([thread = Thread_, queue = Queue_] {
            thread->Stop();
            queue->Drain();
        }));
        FinalizerInvoker_.Reset();
    }

    const IInvokerPtr& GetInvoker(int index) override
    {
        YT_ASSERT(0 <= index && index < std::ssize(QueueIndexToBucketIndex_));

        EnsuredStarted();
        return Queue_->GetInvoker(
            QueueIndexToBucketIndex_[index],
            QueueIndexToBucketQueueIndex_[index]);
    }

    void Reconfigure(const THashMap<TString, double>& newBucketWeights) override
    {
        std::vector<double> weights(BucketNames_.size());
        for (int bucketIndex = 0; bucketIndex < std::ssize(BucketNames_); ++bucketIndex) {
            const auto& bucketName = BucketNames_[bucketIndex];
            if (auto it = newBucketWeights.find(bucketName); it != newBucketWeights.end()) {
                weights[bucketIndex] = it->second;
            } else {
                weights[bucketIndex] = 1.0;
            }
        }

        Queue_->Reconfigure(std::move(weights));
    }

private:
    TFairShareInvokerQueuePtr Queue_;
    TFairShareQueueSchedulerThreadPtr Thread_;

    const TIntrusivePtr<TEventCount> CallbackEventCount_ = New<TEventCount>();

    std::vector<int> QueueIndexToBucketIndex_;
    std::vector<int> QueueIndexToBucketQueueIndex_;

    std::vector<TString> BucketNames_;

    std::atomic<bool> Started_ = false;
    std::atomic<bool> Stopped_ = false;

    IInvokerPtr FinalizerInvoker_ = GetFinalizerInvoker();


    void EnsuredStarted()
    {
        if (Started_.load(std::memory_order_relaxed)) {
            return;
        }
        if (Started_.exchange(true)) {
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
