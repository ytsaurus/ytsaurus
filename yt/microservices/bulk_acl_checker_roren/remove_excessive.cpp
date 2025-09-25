#include <yt/microservices/bulk_acl_checker_roren/remove_excessive.h>

#include <yt/microservices/lib/cpp/get_snapshots/get_snapshots.h>

#include <yt/yt/client/hedging/rpc.h>

#include <library/cpp/yson/node/node_io.h>
#include <util/system/env.h>

TRemoveExcessiveMain::TRemoveExcessiveMain(const NLastGetopt::TOpts& opts)
    : Opts_(opts)
{
}

void TRemoveExcessiveMain::ParseArgs(int argc, const char** argv)
{
    NLastGetopt::TOptsParseResult r(&Opts_, argc, argv);

    Y_ABORT_IF(r.GetFreeArgCount() > 1); // Where was a call opts.SetFreeArgsMax(1) before. This Y_ABORT_IF checks that it has actually been executed.
    if (r.GetFreeArgCount() == 1) {
        Options_.Cluster = r.GetFreeArgs()[0];
    } else {
        Options_.Cluster = GetEnv("YT_PROXY", "");
        if (Options_.Cluster.empty()) {
            Opts_.PrintUsage(argv[0], Cerr);
            exit(1);
        }
    }
    Options_.Destination = r.Get("destination");
    Options_.DaysToLeave = FromString<i64>(r.Get("days-to-leave"));
}

int TRemoveExcessiveMain::RemoveExcessive()
{
    auto secondsToLeave = Options_.DaysToLeave * (24 * 60 * 60);

    const auto ytClient = NYT::CreateClient(Options_.Cluster);
    THashMap<i64, THashMap<TString, i64>> snapshots;
    std::vector<i64> snapshotTimes;

    auto attr = NYT::TAttributeFilter();
    attr.AddAttribute("resource_usage");
    auto listOptions = NYT::TListOptions().AttributeFilter(attr);
    auto [snapshotAttrs, errors] = GetSnapshots(ytClient, Options_.Destination, listOptions);
    for (const auto& error : errors) {
        Cerr << error << Endl;
    }

    for (const auto& [timestampAndSnapshotId, extensionsMap] : snapshotAttrs) {
        const auto& [timestamp, snapshotId] = timestampAndSnapshotId;
        snapshotTimes.push_back(timestamp);
        snapshots[snapshotTimes.back()] = {};
        for (const auto& [extension, attrs] : extensionsMap) {
            auto attrsMap = attrs.AsMap();
            Y_ABORT_IF(!attrsMap.contains("resource_usage"));
            auto& resourceUsage = attrsMap["resource_usage"];
            Y_ABORT_IF(!resourceUsage.IsMap());
            auto& resourceUsageMap = resourceUsage.AsMap();
            Y_ABORT_IF(!resourceUsageMap.contains("disk_space"));
            Y_ABORT_IF(!resourceUsageMap["disk_space"].IsInt64());
            i64 diskSpace = resourceUsageMap["disk_space"].AsInt64();
            snapshots[snapshotTimes.back()][NYT::Format("%v/%v.%v", Options_.Destination, snapshotId, extension)] = diskSpace;
        }
    }

    i64 deletionBorder = Now().TimeT() - secondsToLeave;
    std::vector<i64> timesToRemove;
    std::copy_if(snapshotTimes.begin(), snapshotTimes.end(), std::back_inserter(timesToRemove), [&deletionBorder](i64 snapshotTime) {
        return snapshotTime < deletionBorder;
    });

    for (const auto& snapshotTime : timesToRemove) {
        auto tx = ytClient->StartTransaction();
        for (const auto& [table, diskSize] : snapshots[snapshotTime]) {
            Cout << NYT::Format("Removing table [%v] with disk_size of %v", table, diskSize) << Endl;
            tx->Remove(table);
        }
        tx->Commit();
    }

    Cout << NYT::Format("Done, %v snapshots left.", snapshotTimes.size() - timesToRemove.size()) << Endl;

    return 0;
}

int TRemoveExcessiveMain::operator()(int argc, const char** argv)
{
    ParseArgs(argc, argv);
    return RemoveExcessive();
}
