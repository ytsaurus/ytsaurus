#include "pipeline.h"

#include "metrics.h"

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/json/json_reader.h>

NRoren::TPCollection<NRoren::TKV<ui64, TIdToPathRow>> ApplyMapper(NRoren::TPCollection<std::string> input, std::string forceCluster, THashSet<std::string> allowClusters)
{
    static auto Logger = NYT::NLogging::TLogger("IdToPathUpdater");
    NRoren::TPCollection<TIdToPathRow> parsed = input | NRoren::ParDo([] (const std::string& json, NRoren::TOutput<NJson::TJsonValue>& output) {
        try {
            TMemoryInput memoryInput(json);
            output.Add(NJson::ReadJsonTree(&memoryInput));
        } catch (const std::exception& ex) {
            YT_LOG_WARNING("Cannot parse json value: %v", ex.what());
        }
    })
    | LogOnceInAWhile("json value")
    | MakeUpdateItem(std::move(forceCluster))
    | NRoren::MakeParDo<TClusterMetricsFn>("parsed");

    NRoren::TPCollection<TIdToPathRow> filtered = parsed;
    if (!allowClusters.empty()) {
        filtered = filtered | AllowClusters(std::move(allowClusters));
    }

    return filtered
        | NRoren::MakeParDo<TClusterMetricsFn>("filtered")
        | NRoren::ParDo([](TIdToPathRow&& row, NRoren::TOutput<NRoren::TKV<ui64, TIdToPathRow>>& output) {
            auto hash = NYT::FarmFingerprint(
                NYT::FarmHash(row.GetCluster().data(), row.GetCluster().size()),
                NYT::FarmHash(row.GetNodeId().data(), row.GetNodeId().size())
            );
            output.Add({hash, std::move(row)});
        })
        | LogOnceInAWhile("intermediate proto");
}
