#include "id_to_path_updater.h"

#include <yt/yt/core/ypath/helpers.h>

#include <library/cpp/json/json_reader.h>

#include <util/system/env.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

static auto Logger = NYT::NLogging::TLogger("IdToPathUpdater");

////////////////////////////////////////////////////////////////////////////////

TLogOnceInAWhileTransform LogOnceInAWhile(TString objectName)
{
    return {std::move(objectName)};
}

////////////////////////////////////////////////////////////////////////////////

NRoren::TParDoApplicator<NJson::TJsonValue, TIdToPathRow> MakeUpdateItem(TString forceCluster)
{
    return NRoren::ParDo([] (const NJson::TJsonValue& jsonValue, NRoren::TOutput<TIdToPathRow>& output, const TString& forceCluster) {
        auto getStringOrEmpty = [] (const NJson::TJsonValue& jsonValue, TStringBuf key) {
            TString result;
            const NJson::TJsonValue* child;
            if (!jsonValue.GetValuePointer(key, &child)) {
                return result;
            }
            child->GetString(&result);
            return result;
        };
        TString method = getStringOrEmpty(jsonValue, "method");
        TString cluster = forceCluster.empty() ? getStringOrEmpty(jsonValue, "cluster") : forceCluster;
        TString id = getStringOrEmpty(jsonValue, "id");

        TString path;

        enum EInterestingArgument {
            None,
            Path,
            DestinationPath
        };
        static const THashMap<TString, EInterestingArgument> argumentMap = {
            {"Create", EInterestingArgument::Path},
            {"Get", EInterestingArgument::Path},
            {"GetBasicAttributes", EInterestingArgument::Path},
            {"List", EInterestingArgument::Path},
            {"Remove", EInterestingArgument::Path},
            {"Set", EInterestingArgument::Path},

            {"Copy", EInterestingArgument::DestinationPath},
            {"Link", EInterestingArgument::DestinationPath},
            {"Move", EInterestingArgument::DestinationPath},
        };

        auto interestingArgument = argumentMap.Value(method, EInterestingArgument::None);

        if (interestingArgument == EInterestingArgument::Path) {
            path = getStringOrEmpty(jsonValue, "path");
            if (path.StartsWith("#")) {
                return;
            }
        } else if (interestingArgument == EInterestingArgument::DestinationPath) {
            path = getStringOrEmpty(jsonValue, "destination_path");
            if (path.StartsWith("#")) {
                path = getStringOrEmpty(jsonValue, "original_destination_path");
            }

            if (path.StartsWith("#")) {
                return;
            }
        } else {
            return;
        }
        path = NYT::NYPath::StripAttributes(path);
        if (path.empty()) {
            return;
        }

        TIdToPathRow record;
        record.SetCluster(cluster);
        record.SetNodeId(id);
        record.SetPath(path);

        output.Add(std::move(record));
    }, std::move(forceCluster));
}

NRoren::TParDoApplicator<TIdToPathRow, TIdToPathRow> AllowClusters(THashSet<std::string> clusters)
{
    return NRoren::ParDo([](TIdToPathRow&& input, NRoren::TOutput<TIdToPathRow>& output, const THashSet<std::string>& clusters) {
        if (clusters.contains(input.GetCluster())) {
            output.Add(std::move(input));
        }
    }, std::move(clusters));
}

NRoren::TParDoApplicator<TIdToPathRow, TIdToPathRow> DenyClusters(THashSet<std::string> clusters)
{
    return NRoren::ParDo([](TIdToPathRow&& input, NRoren::TOutput<TIdToPathRow>& output, const THashSet<std::string>& clusters) {
        if (!clusters.contains(input.GetCluster())) {
            output.Add(std::move(input));
        }
    }, std::move(clusters));
}
