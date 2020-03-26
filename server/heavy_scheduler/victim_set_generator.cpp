#include "victim_set_generator.h"

#include "disruption_throttler.h"
#include "private.h"
#include "resource_vector.h"

#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/node.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

////////////////////////////////////////////////////////////////////////////////

class TSinglePodGenerator
    : public IVictimSetGenerator
{
public:
    TSinglePodGenerator(
        TNode* victimNode,
        TPod* starvingPod,
        TDisruptionThrottlerPtr disruptionThrottler,
        bool verbose)
        : VictimNode_(victimNode)
        , StarvingPod_(starvingPod)
        , DisruptionThrottler_(std::move(disruptionThrottler))
        , Verbose_(verbose)
        , Exhausted_(false)
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
    const TNode* VictimNode_;
    const TPod* StarvingPod_;
    const TDisruptionThrottlerPtr DisruptionThrottler_;
    const bool Verbose_;
    bool Exhausted_;

    TPod* FindVictimPod()
    {
        for (auto* victimPod : GetNodeSchedulablePods()) {
            auto starvingPodResourceVector = GetResourceRequestVector(StarvingPod_);
            auto victimPodResourceVector = GetResourceRequestVector(victimPod);
            auto freeNodeResourceVector = GetFreeResourceVector(VictimNode_);
            if (freeNodeResourceVector + victimPodResourceVector < starvingPodResourceVector) {
                YT_LOG_DEBUG_IF(Verbose_,
                    "Not enough resources according to resource vectors "
                    "(NodeId: %v, VictimPodId: %v, StarvingPodId: %v)",
                    VictimNode_->GetId(),
                    victimPod->GetId(),
                    StarvingPod_->GetId());
                continue;
            }

            YT_LOG_DEBUG_IF(Verbose_,
                "Checking eviction safety (PodId: %v)",
                victimPod->GetId());
            if (DisruptionThrottler_->ThrottleEviction(victimPod)) {
                continue;
            }

            return victimPod;
        }

        return nullptr;
    }

    std::vector<TPod*> GetNodeSchedulablePods()
    {
        std::vector<TPod*> pods;
        for (auto* pod : VictimNode_->Pods()) {
            if (pod->GetEnableScheduling()) {
                pods.push_back(pod);
            }
        }
        return pods;
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
        default:
            YT_UNIMPLEMENTED();
    }

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
