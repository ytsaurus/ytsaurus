#include "pipeline_description_unroll.h"

#include "graph_entity_id.h"

#include <algorithm>
#include <string>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

namespace {

TGraphEntityId AppendSuffix(const TGraphEntityId& id, const std::string& suffix)
{
    return TGraphEntityId(std::string(id.Underlying()) + suffix);
}

TComputationId AppendSuffix(const TComputationId& id, const std::string& suffix)
{
    return TComputationId(std::string(id.Underlying()) + suffix);
}

std::vector<TStreamEdge> KeepStreamEdgesForId(const std::vector<TStreamEdge>& edges, const TGraphEntityId& keep)
{
    std::vector<TStreamEdge> result;
    for (const auto& edge : edges) {
        if (edge.StreamGraphEntityId == keep) {
            result.push_back(edge);
        }
    }
    return result;
}

THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>> KeepLimitStatsForId(
    const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& stats,
    const TGraphEntityId& keep)
{
    THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>> result;
    if (auto it = stats.find(keep); it != stats.end()) {
        result.emplace(it->first, it->second);
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TPipelineDescription UnrollPipelineDescription(const TPipelineDescription& original)
{
    TPipelineDescription result;
    result.Status = original.Status;
    result.Messages = original.Messages;
    result.Streams = original.Streams;
    result.Sources = original.Sources;
    result.Sinks = original.Sinks;
    result.ReadDelayEdges = original.ReadDelayEdges;

    for (const auto& [computationId, computation] : original.Computations) {
        if (computation.StreamsDependency.empty()) {
            result.Computations[computationId] = computation;
            continue;
        }

        // Flatten StreamsDependency into a sorted vector of (out, in) pairs for determinism.
        std::vector<std::pair<TGraphEntityId, TGraphEntityId>> pairs;
        for (const auto& [outId, inSet] : computation.StreamsDependency) {
            for (const auto& inId : inSet) {
                pairs.emplace_back(outId, inId);
            }
        }
        std::sort(pairs.begin(), pairs.end());

        int index = 0;
        for (const auto& [outId, inId] : pairs) {
            ++index;
            std::string suffix = "-unrolled-" + std::to_string(index);

            auto subCopy = computation;
            subCopy.Id = AppendSuffix(computation.Id, suffix);
            subCopy.InputStreams = {inId};
            subCopy.OutputStreams = {outId};
            subCopy.SourceStreams.clear();
            subCopy.TimerStreams.clear();
            subCopy.StreamsDependency.clear();
            subCopy.InputLimitStats = KeepLimitStatsForId(computation.InputLimitStats, inId);
            subCopy.OutputLimitStats = KeepLimitStatsForId(computation.OutputLimitStats, outId);
            subCopy.ExtendedInputStreams = KeepStreamEdgesForId(computation.ExtendedInputStreams, inId);
            subCopy.ExtendedOutputStreams = KeepStreamEdgesForId(computation.ExtendedOutputStreams, outId);
            subCopy.ExtendedSourceStreams.clear();
            subCopy.ExtendedTimerStreams.clear();
            // Keep Messages and Status only on the first sub-copy to avoid duplicating
            // the same warning across all N unrolled sub-nodes.
            if (index > 1) {
                subCopy.Messages.clear();
                subCopy.Status = NLogging::ELogLevel::Info;
            }

            auto syntheticId = AppendSuffix(computationId, "_unrolled_" + std::to_string(index));
            result.Computations[syntheticId] = std::move(subCopy);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
