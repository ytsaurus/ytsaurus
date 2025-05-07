#include <deque>

#include <yt/yt/server/master/node_tracker_server/public.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/rack.h>
#include <yt/yt/library/erasure/public.h>

namespace NYT::NChunkServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NNodeTrackerServer;

class TPackingMaxErasureReplicasPerRack : public virtual TRefCounted {
public:
    TPackingMaxErasureReplicasPerRack(TDuration stabilizationWindow, INodeTrackerPtr nodeTracker):
        StabilizationWindow_(stabilizationWindow), NodeTracker_(std::move(nodeTracker))
    {
        NodeTracker_->SubscribeNodeUnregistered(BIND_NO_PROPAGATE(&TPackingMaxErasureReplicasPerRack::OnNodeUnregistered, MakeWeak(this)));
    }

    int Get(int mediumIndex, NErasure::ECodec erasureCodec) {
        auto state = Mediums_.find(mediumIndex);
        return state == Mediums_.end() ? std::numeric_limits<int>::max() : state->second.Get(erasureCodec);
    }

    void OnNodeHeartbeat(TNode* node) {
        for (int medium = 0; medium < MaxMediumCount; medium++) {
            if (node->HasMedium(medium) || Mediums_.contains(medium)) {
                Mediums_.try_emplace(medium, this).first->second.OnNodeHeartbeat(medium, node);
            }
        }
    }

private:
    struct MediumIndexState {
        TPackingMaxErasureReplicasPerRack *Outer;
        std::deque<std::pair<TInstant, size_t>> InstantToIncreasingCount;
        std::unordered_map<TRack*, std::unordered_set<TNode*>> RacksNotFullNodes;

        int Get(NErasure::ECodec erasureCodec) {
            Tick();
            int parts;
            switch (erasureCodec) {
                case NErasure::ECodec::ReedSolomon_3_3:
                    parts = 6;
                    break;
                case NErasure::ECodec::ReedSolomon_6_3: [[fallthrough]];
                case NErasure::ECodec::IsaReedSolomon_6_3:
                    parts = 9;
                    break;
                case NErasure::ECodec::IsaLrc_12_2_2:
                    parts = 16;
                    break;
                default:
                    return std::numeric_limits<int>::max();
            }
            auto min_racks = InstantToIncreasingCount.empty() ? RacksNotFullNodes.size() : InstantToIncreasingCount.front().second;
            return min_racks ? (parts - 1) / min_racks + 1 : std::numeric_limits<int>::max();
        }

        void Tick() {
            auto deadline = TInstant::Now() - Outer->StabilizationWindow_;
            while (!InstantToIncreasingCount.empty() && InstantToIncreasingCount.front().first <= deadline) {
                InstantToIncreasingCount.pop_front();
            }
        }

        void OnNodeHeartbeat(int mediumIndex, TNode* node) {
            auto rack = node->GetRack();
            if (rack && node->HasNotFullMedium(mediumIndex)) {
                auto size = RacksNotFullNodes.size();
                auto &nodes = RacksNotFullNodes[rack];
                nodes.insert(node);
                if (size != RacksNotFullNodes.size()) {
                    Tick();
                    InstantToIncreasingCount.push_back({TInstant::Now(), size});
                }
            }
        }

        void OnNodeUnregistered(TNode* node) {
            if (auto rack = node->GetRack()) {
                auto &nodes = RacksNotFullNodes[rack];
                nodes.erase(node);
                if (nodes.empty()) {
                    RacksNotFullNodes.erase(rack);
                    Tick();
                    if (!InstantToIncreasingCount.empty() && InstantToIncreasingCount.back().second == RacksNotFullNodes.size()) {
                        InstantToIncreasingCount.pop_back();
                    }
                }
            }
        }
    };

    TDuration StabilizationWindow_;
    INodeTrackerPtr NodeTracker_;
    std::unordered_map<int, MediumIndexState> Mediums_;

    void OnNodeUnregistered(TNode* node) {
        for (int medium = 0; medium < MaxMediumCount; medium++) {
            if (node->HasMedium(medium) || Mediums_.contains(medium)) {
                Mediums_.try_emplace(medium, this).first->second.OnNodeUnregistered(node);
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TPackingMaxErasureReplicasPerRack)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
