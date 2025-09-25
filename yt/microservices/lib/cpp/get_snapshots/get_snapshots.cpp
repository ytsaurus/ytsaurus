#include <yt/microservices/lib/cpp/get_snapshots/get_snapshots.h>
#include <yt/yt/client/hedging/rpc.h>

std::pair<TMap<std::pair<i64, TString>, THashMap<TString, NYT::TNode>>, std::vector<TString>> GetSnapshots(
    NYT::IClientPtr ytClient,
    const TString& basePath,
    const NYT::TListOptions& listOptions)
{
    auto ytList = ytClient->List(basePath, listOptions);

    std::vector<TString> errors;
    TMap<std::pair<i64, TString>, THashMap<TString, NYT::TNode>> timestampMapping;
    for (const auto& resultElement: ytList) {
        Y_ABORT_IF(!resultElement.IsString());
        const TStringBuf path = resultElement.AsString();
        if (path == "tmp") {
            continue;
        }
        TStringBuf snapshotId, extension;
        if (!path.TryRSplit(".", snapshotId, extension)) {
            errors.push_back(NYT::Format("Could not separate extension from %v.", path));
            continue;
        }
        TStringBuf secondsOfTs, millisecondOfTs;
        if (!snapshotId.TryRSplit(":", secondsOfTs, millisecondOfTs)) {
            errors.push_back(NYT::Format("Could not timestamp from %v.", snapshotId));
            continue;
        }
        try {
            i64 timestamp = FromString<i64>(secondsOfTs);
            timestampMapping[std::pair<i64, TString>{timestamp, snapshotId}][extension] = resultElement.GetAttributes();
        } catch (const std::exception& e) {
            errors.push_back(e.what());
        }
    }

    return {timestampMapping, errors};
}
