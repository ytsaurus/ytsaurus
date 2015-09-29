#ifndef STATISTICS_INL_H_
#error "Direct inclusion of this file is not allowed, include statistics.h"
#endif

#include <core/ypath/tokenizer.h>

namespace NYT {
namespace NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TStatistics::AddSample(const NYPath::TYPath& path, const T& sample)
{
    AddSample(path, NYTree::ConvertToNode(sample));
}

template <class T>
T GetValues(
    const TStatistics& statistics, 
    const NYPath::TYPath& path,
    std::function<i64(const TSummary&)> getValue)
{
    NYTree::INodePtr root = NYTree::GetEphemeralNodeFactory()->CreateMap();
    for (const auto& pair : statistics.Data()) {
        if (NYPath::HasPrefix(pair.first, path)) {
            const auto& summary = pair.second;
            auto subPath = pair.first.substr(path.size());
            if (subPath.empty()) {
                root = NYTree::ConvertToNode(getValue(summary));
            } else {
                ForceYPath(root, subPath);
                SetNodeByYPath(root, subPath, NYTree::ConvertToNode(getValue(summary)));
            }
        }
    }

    T result;
    Deserialize(result, std::move(root));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
