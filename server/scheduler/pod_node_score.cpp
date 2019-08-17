#include "pod_node_score.h"
#include "pod.h"
#include "node.h"
#include "config.h"
#include "helpers.h"

#include <yt/core/ytree/yson_serializable.h>

#include <util/digest/murmur.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// TODO(bidzilya): Move TNodeResourceCapacities and TNodeResources to the
//                 scheduler/node.h and generalize allocator code.
struct TNodeResourceCapacities
{
    TResourceCapacities Cpu = {};
    TResourceCapacities Memory = {};
};

TNodeResourceCapacities& operator += (TNodeResourceCapacities& lhs, const TNodeResourceCapacities& rhs)
{
    lhs.Cpu += rhs.Cpu;
    lhs.Memory += rhs.Memory;
    return lhs;
}

TNodeResourceCapacities GetPodResourceRequestCapacities(TPod* pod)
{
    const auto& resourceRequests = pod->ResourceRequests();
    return {
        MakeCpuCapacities(resourceRequests.vcpu_guarantee()),
        MakeMemoryCapacities(resourceRequests.memory_limit())
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TNodeResources
{
    THomogeneousResource Cpu = {};
    THomogeneousResource Memory = {};

    bool TryAllocate(TPod* pod)
    {
        auto podResourceRequestCapacities = GetPodResourceRequestCapacities(pod);
        if (!Cpu.CanAllocate(podResourceRequestCapacities.Cpu)
            || !Memory.CanAllocate(podResourceRequestCapacities.Memory))
        {
            return false;
        }
        YT_VERIFY(Cpu.TryAllocate(podResourceRequestCapacities.Cpu));
        YT_VERIFY(Memory.TryAllocate(podResourceRequestCapacities.Memory));
        return true;
    }

    TNodeResourceCapacities GetTotalCapacities() const
    {
        return {
            Cpu.GetTotalCapacities(),
            Memory.GetTotalCapacities()
        };
    }

    TNodeResourceCapacities GetFreeCapacities() const
    {
        return {
            Cpu.GetFreeCapacities(),
            Memory.GetFreeCapacities()
        };
    }
};

TNodeResources GetNodeResources(TNode* node)
{
    return {
        node->CpuResource(),
        node->MemoryResource()
    };
}

////////////////////////////////////////////////////////////////////////////////

class TNodeRandomHashPodNodeScoreParameters
    : public NYT::NYTree::TYsonSerializable
{
public:
    ui32 Seed;

    TNodeRandomHashPodNodeScoreParameters()
    {
        RegisterParameter("seed", Seed)
            .Default(42);
    }
};

DECLARE_REFCOUNTED_CLASS(TNodeRandomHashPodNodeScoreParameters)
DEFINE_REFCOUNTED_TYPE(TNodeRandomHashPodNodeScoreParameters)

////////////////////////////////////////////////////////////////////////////////

class TNodeRandomHashPodNodeScore
    : public IPodNodeScore
{
public:
    explicit TNodeRandomHashPodNodeScore(TNodeRandomHashPodNodeScoreParametersPtr parameters)
        : Parameters_(std::move(parameters))
    { }

    virtual TPodNodeScoreValue Compute(TNode* node, TPod* /*pod*/) override
    {
        const auto& nodeId = node->GetId();
        return MurmurHash(static_cast<const void*>(nodeId.data()), nodeId.size(), Parameters_->Seed);
    }

private:
    const TNodeRandomHashPodNodeScoreParametersPtr Parameters_;
};

////////////////////////////////////////////////////////////////////////////////

double GetMean(std::initializer_list<double> values)
{
    YT_VERIFY(values.size() > 0);
    return std::accumulate(values.begin(), values.end(), 0.0) / values.size();
}

double GetVariance(std::initializer_list<double> values)
{
    YT_VERIFY(values.size() > 0);
    auto mean = GetMean(values);
    auto variance = 0.0;
    for (auto value : values) {
        variance += (value - mean) * (value - mean);
    }
    return variance / values.size();
}

////////////////////////////////////////////////////////////////////////////////

class TFreeCpuMemorySharePodNodeScoreBase
    : public IPodNodeScore
{
protected:
    static double ComputeShare(ui64 freeResourceCapacity, ui64 totalResourceCapacity)
    {
        YT_VERIFY(freeResourceCapacity <= totalResourceCapacity);
        YT_VERIFY(freeResourceCapacity >= 0);
        return totalResourceCapacity == 0
            ? 1.0
            : static_cast<double>(freeResourceCapacity) / totalResourceCapacity;
    }

    static std::pair<double, double> ComputeFreeCpuMemoryShares(const TNodeResources& nodeResources)
    {
        auto nodeFreeResourceCapacities = nodeResources.GetFreeCapacities();
        auto nodeTotalResourceCapacities = nodeResources.GetTotalCapacities();

        auto freeCpuShare = ComputeShare(
            GetCpuCapacity(nodeFreeResourceCapacities.Cpu),
            GetCpuCapacity(nodeTotalResourceCapacities.Cpu));

        auto freeMemoryShare = ComputeShare(
            GetMemoryCapacity(nodeFreeResourceCapacities.Memory),
            GetMemoryCapacity(nodeTotalResourceCapacities.Memory));

        return std::make_pair(freeCpuShare, freeMemoryShare);
    }

    static void AllocateOrThrow(
        const TObjectId& nodeId,
        TNodeResources* nodeResources,
        TPod* pod)
    {
        if (!nodeResources->TryAllocate(pod)) {
            THROW_ERROR_EXCEPTION(
                "Could not allocate resources for pod %Qv on node %Qv while computing pod node score",
                pod->GetId(),
                nodeId);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFreeCpuMemoryShareVariancePodNodeScore
    : public TFreeCpuMemorySharePodNodeScoreBase
{
public:
    virtual TPodNodeScoreValue Compute(TNode* node, TPod* pod) override
    {
        auto nodeResources = GetNodeResources(node);
        AllocateOrThrow(node->GetId(), &nodeResources, pod);
        auto [freeCpuShare, freeMemoryShare] = ComputeFreeCpuMemoryShares(nodeResources);
        return GetVariance({freeCpuShare, freeMemoryShare});
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFreeCpuMemoryShareSquaredMinDeltaPodNodeScore
    : public TFreeCpuMemorySharePodNodeScoreBase
{
public:
    virtual TPodNodeScoreValue Compute(TNode* node, TPod* pod) override
    {
        auto nodeResources = GetNodeResources(node);
        auto freeCpuMemoryShareMinBefore = ComputeFreeCpuMemoryShareMin(nodeResources);
        AllocateOrThrow(node->GetId(), &nodeResources, pod);
        auto freeCpuMemoryShareMinAfter = ComputeFreeCpuMemoryShareMin(nodeResources);
        return freeCpuMemoryShareMinBefore * freeCpuMemoryShareMinBefore
            - freeCpuMemoryShareMinAfter * freeCpuMemoryShareMinAfter;
    }

private:
    static double ComputeFreeCpuMemoryShareMin(const TNodeResources& nodeResources)
    {
        auto [freeCpuShare, freeMemoryShare] = ComputeFreeCpuMemoryShares(nodeResources);
        return std::min(freeCpuShare, freeMemoryShare);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TPodNodeScoreParameters>
TIntrusivePtr<TPodNodeScoreParameters> CreatePodNodeScoreParameters(
    const NYT::NYTree::INodePtr& dynamicParameters)
{
    auto parameters = New<TPodNodeScoreParameters>();
    parameters->SetUnrecognizedStrategy(NYT::NYTree::EUnrecognizedStrategy::KeepRecursive);
    parameters->Load(dynamicParameters);
    return parameters;
}

////////////////////////////////////////////////////////////////////////////////

IPodNodeScorePtr CreatePodNodeScore(TPodNodeScoreConfigPtr config)
{
    switch (config->Type) {
        case EPodNodeScoreType::NodeRandomHash:
            return New<TNodeRandomHashPodNodeScore>(
                CreatePodNodeScoreParameters<TNodeRandomHashPodNodeScoreParameters>(
                    config->Parameters));
        case EPodNodeScoreType::FreeCpuMemoryShareVariance:
            return New<TFreeCpuMemoryShareVariancePodNodeScore>();
        case EPodNodeScoreType::FreeCpuMemoryShareSquaredMinDelta:
            return New<TFreeCpuMemoryShareSquaredMinDeltaPodNodeScore>();
        default:
            YT_UNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
