#include "node_score.h"

#include "config.h"
#include "private.h"

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/object_filter_cache.h>
#include <yp/server/lib/cluster/object_filter_evaluator.h>

#include <yp/server/lib/objects/object_filter.h>

namespace NYP::NServer::NScheduler {

using namespace NCluster;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TNodeFeature
{
public:
    void Clear()
    {
        IdToFeature_.clear();
    }

    void Initialize(
        const std::vector<TNode*>& nodes,
        const std::vector<TNode*>& featuredNodes)
    {
        IdToFeature_.clear();
        IdToFeature_.reserve(nodes.size());
        for (auto* node : nodes) {
            YT_VERIFY(IdToFeature_.emplace(node->GetId(), false).second);
        }
        for (auto* featuredNode : featuredNodes) {
            auto it = IdToFeature_.find(featuredNode->GetId());
            YT_VERIFY(it != IdToFeature_.end());
            YT_VERIFY(it->second == false);
            it->second = true;
        }
    }

    bool HasFeature(TNode* node) const
    {
        auto it = IdToFeature_.find(node->GetId());
        YT_VERIFY(it != IdToFeature_.end());
        return it->second;
    }

private:
    THashMap<TObjectId, bool> IdToFeature_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TNodeScore
    : public INodeScore
{
public:
    TNodeScore(TNodeScoreConfigPtr config, IObjectFilterEvaluatorPtr nodeFilterEvaluator)
        : Config_(std::move(config))
        , NodeFilterEvaluator_(std::move(nodeFilterEvaluator))
        , Features_(GetFeatureCount())
    { }

    virtual void ReconcileState(const TClusterPtr& cluster) override
    {
        YT_LOG_DEBUG("Reconciling node score state (FeatureCount: %v)",
            GetFeatureCount());

        try {
            ReconcileFeaturesState(cluster);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error reconciling node score state")
                << ex;
        }
    }

    virtual TNodeScoreValue Compute(TNode* node) override
    {
        TNodeScoreValue result = 0;
        for (int i = 0; i < GetFeatureCount(); ++i) {
            const auto& config = GetFeatureConfig(i);

            if (Features_[i].HasFeature(node)) {
                result += config->Weight;
            }
        }
        return result;
    }

private:
    const TNodeScoreConfigPtr Config_;
    const IObjectFilterEvaluatorPtr NodeFilterEvaluator_;

    std::vector<TNodeFeature> Features_;


    const TNodeScoreFeatureConfigPtr& GetFeatureConfig(int index) const
    {
        YT_VERIFY(0 <= index && index < static_cast<int>(Config_->Features.size()));
        return Config_->Features[index];
    }

    int GetFeatureCount() const
    {
        return static_cast<int>(Config_->Features.size());
    }


    void ReconcileFeaturesState(const TClusterPtr& cluster)
    {
        if (!GetFeatureCount()) {
            YT_LOG_DEBUG("Empty list of node score features; skipping state reconciliation");
            return;
        }

        for (auto& feature : Features_) {
            feature.Clear();
        }

        auto nodes = cluster->GetNodes();

        TObjectFilterCache<TNode> cache(
            NodeFilterEvaluator_,
            nodes);

        for (int i = 0; i < GetFeatureCount(); ++i) {
            const auto& config = GetFeatureConfig(i);

            YT_LOG_DEBUG("Reconciling node score feature state (FilterQuery: %v, Weight: %v)",
                config->FilterQuery,
                config->Weight);

            const auto& featuredNodesOrError = cache.Get(NObjects::TObjectFilter{config->FilterQuery});
            THROW_ERROR_EXCEPTION_IF_FAILED(featuredNodesOrError, "Error filtering nodes by feature filter (FilterQuery: %v)",
                config->FilterQuery);

            Features_[i].Initialize(
                nodes,
                featuredNodesOrError.Value());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeScorePtr CreateNodeScore(
    TNodeScoreConfigPtr config,
    IObjectFilterEvaluatorPtr nodeFilterEvaluator)
{
    return New<TNodeScore>(std::move(config), std::move(nodeFilterEvaluator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
