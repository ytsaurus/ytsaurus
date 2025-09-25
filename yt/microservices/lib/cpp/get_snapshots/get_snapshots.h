#include <yt/cpp/mapreduce/interface/client.h>

std::pair<TMap<std::pair<i64, TString>, THashMap<TString, NYT::TNode>>, std::vector<TString>> GetSnapshots(
    NYT::IClientPtr ytClient,
    const TString& basePath,
    const NYT::TListOptions& listOptions);
