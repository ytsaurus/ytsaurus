#include "victim_set_generator.h"

#include "disruption_throttler.h"
#include "private.h"
#include "resource_vector.h"

#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/node.h>

#include <util/random/shuffle.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

////////////////////////////////////////////////////////////////////////////////

std::vector<TPod*> GetNodeSchedulablePods(const TNode* node)
{
    std::vector<TPod*> pods;
    for (auto* pod : node->Pods()) {
        if (pod->GetEnableScheduling()) {
            pods.push_back(pod);
        }
    }
    return pods;
}

std::vector<TObjectId> GetPodIds(const std::vector<TPod*>& pods)
{
    std::vector<TObjectId> podIds;
    podIds.reserve(pods.size());
    for (auto* pod : pods) {
        podIds.push_back(pod->GetId());
    }
    return podIds;
}

////////////////////////////////////////////////////////////////////////////////

class TVictimSetGeneratorBase
    : public IVictimSetGenerator
{
protected:
    const TNode* VictimNode_;
    const TPod* StarvingPod_;
    const TDisruptionThrottlerPtr DisruptionThrottler_;
    const bool Verbose_;

    TVictimSetGeneratorBase(
        TNode* victimNode,
        TPod* starvingPod,
        TDisruptionThrottlerPtr disruptionThrottler,
        bool verbose)
        : VictimNode_(victimNode)
        , StarvingPod_(starvingPod)
        , DisruptionThrottler_(std::move(disruptionThrottler))
        , Verbose_(verbose)
    { }

    bool HasEnoughResources(const std::vector<TPod*>& pods) const
    {
        auto starvingPodResourceVector = GetResourceRequestVector(StarvingPod_);
        auto freeNodeResourceVector = GetFreeResourceVector(VictimNode_);
        for (const auto& pod : pods) {
            freeNodeResourceVector += GetResourceRequestVector(pod);
        }
        if (freeNodeResourceVector < starvingPodResourceVector) {
            YT_LOG_DEBUG_IF(Verbose_,
                "Not enough resources according to resource vectors "
                "(NodeId: %v, StarvingPodId: %v, VictimPodIds: %v)",
                VictimNode_->GetId(),
                StarvingPod_->GetId(),
                GetPodIds(pods));
            return false;
        }
        return true;
    }

    bool IsSafeToEvict(const std::vector<TPod*>& pods)
    {
        YT_LOG_DEBUG_IF(Verbose_,
            "Checking eviction safety (PodIds: %v)",
            GetPodIds(pods));
        return !DisruptionThrottler_->ThrottleEviction(pods);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSinglePodGenerator
    : public TVictimSetGeneratorBase
{
public:
    TSinglePodGenerator(
        TNode* victimNode,
        TPod* starvingPod,
        TDisruptionThrottlerPtr disruptionThrottler,
        bool verbose)
        : TVictimSetGeneratorBase(victimNode, starvingPod, std::move(disruptionThrottler), verbose)
    { }

    virtual std::vector<TPod*> GetNextCandidates() override
    {
        if (Exhausted_) {
            return {};
        }
        Exhausted_ = true;
        if (auto* victimPod = FindVictimPod()) {
            return {victimPod};
        }
        return {};
    }

private:
    bool Exhausted_ = false;

    TPod* FindVictimPod()
    {
        std::vector<TPod*> candidate = {nullptr};
        for (auto* victimPod : GetNodeSchedulablePods(VictimNode_)) {
            candidate[0] = victimPod;
            if (HasEnoughResources(candidate) && IsSafeToEvict(candidate)) {
                return victimPod;
            }
        }

        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBruteForceGenerator
    : public TVictimSetGeneratorBase
{
public:
    TBruteForceGenerator(
        TNode* victimNode,
        TPod* starvingPod,
        TDisruptionThrottlerPtr disruptionThrottler,
        bool verbose,
        size_t maxSetSize,
        size_t chooseFromSize)
        : TVictimSetGeneratorBase(victimNode, starvingPod, std::move(disruptionThrottler), verbose)
    {
        ChooseFrom_ = GetNodeSchedulablePods(VictimNode_);
        Shuffle(ChooseFrom_.begin(), ChooseFrom_.end());
        if (ChooseFrom_.size() > chooseFromSize) {
            ChooseFrom_.resize(chooseFromSize);
        }

        Selector_ = TSubsetSelector(maxSetSize, ChooseFrom_.size());
    }

    virtual std::vector<TPod*> GetNextCandidates() override
    {
        while (!Selector_.IsExhausted()) {
            auto currentSet = GetCurrentSet();
            if (!IsSafeToEvict(currentSet)) {
                Selector_.SkipBranch();
                continue;
            }
            Selector_.Increment();
            if (HasEnoughResources(currentSet)) {
                return currentSet;
            }
        }
        return {};
    }

private:
    class TSubsetSelector
    {
    public:
        TSubsetSelector()
            : TSubsetSelector(0, 0)
        { }

        TSubsetSelector(size_t maxSetSize, size_t itemsCount)
            : MaxSetSize_(maxSetSize)
            , ItemsCount_(itemsCount)
        {
            if (MaxSetSize_ > 0 && ItemsCount_ > 0) {
                Indicies_ = {0};
            }
        }

        void SkipBranch()
        {
            YT_VERIFY(!IsExhausted());
            IncrementCurrent();
        }

        void Increment()
        {
            YT_VERIFY(!IsExhausted());
            if (Indicies_.size() < MaxSetSize_ && Indicies_.back() + 1 < ItemsCount_) {
                Indicies_.push_back(Indicies_.back() + 1);
            } else {
                IncrementCurrent();
            }
        }

        bool IsExhausted()
        {
            return Indicies_.empty();
        }

        const std::vector<size_t>& GetIndicies() const
        {
            return Indicies_;
        }

    private:
        size_t MaxSetSize_;
        size_t ItemsCount_;
        std::vector<size_t> Indicies_;

        void IncrementCurrent()
        {
            if (Indicies_.back() + 1 < ItemsCount_) {
                Indicies_.back() += 1;
            } else {
                Indicies_.pop_back();
                if (!Indicies_.empty()) {
                    Indicies_.back() += 1;
                }
            }
        }
    };

    std::vector<TPod*> ChooseFrom_;
    TSubsetSelector Selector_;

    std::vector<TPod*> GetCurrentSet() const
    {
        const auto& indicies = Selector_.GetIndicies();
        std::vector<TPod*> result;
        result.reserve(indicies.size());
        for (size_t index : indicies) {
            result.push_back(ChooseFrom_[index]);
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

IVictimSetGeneratorPtr CreateNodeVictimSetGenerator(
    EVictimSetGeneratorType generatorType,
    NCluster::TNode* victimNode,
    NCluster::TPod* starvingPod,
    TDisruptionThrottlerPtr disruptionThrottler,
    bool verbose)
{
    switch (generatorType) {
        case EVictimSetGeneratorType::Single:
            return New<TSinglePodGenerator>(
                victimNode,
                starvingPod,
                std::move(disruptionThrottler),
                verbose);
        case EVictimSetGeneratorType::BruteForce:
            return New<TBruteForceGenerator>(
                victimNode,
                starvingPod,
                std::move(disruptionThrottler),
                verbose,
                /* maxSetSize = */ 3,
                /* chooseFromSize = */ 20);
        default:
            YT_UNIMPLEMENTED();
    }

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
