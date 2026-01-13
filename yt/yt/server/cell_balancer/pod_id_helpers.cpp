#include "pod_id_helpers.h"

#include "cypress_bindings.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

std::string GetPodIdForInstance(const TCypressAnnotationsPtr& cypressAnnotations, const std::string& name)
{
    if (cypressAnnotations && cypressAnnotations->PodId) {
        return *cypressAnnotations->PodId;
    }

    // Fallback to the old hacky way for compatibility:
    // we expect PodId to be prefix of fqdn before the first dot.
    auto endPos = name.find(".");
    if (endPos == std::string::npos && name.starts_with("localhost")) {
        // For testing purposes.
        return name;
    }
    YT_VERIFY(endPos != std::string::npos);

    auto podId = name.substr(0, endPos);
    YT_VERIFY(!podId.empty());
    return podId;
}

std::string GetInstancePodIdTemplate(
    const std::string& cluster,
    const std::string& bundleName,
    const std::string& instanceType,
    int index)
{
    return Format("<short-hostname>-%v-%03x-%v-%v", bundleName, index, instanceType, cluster);
}

std::optional<int> GetIndexFromPodId(
    const std::string& podId,
    const std::string& cluster,
    const std::string& instanceType)
{
    TStringBuf buffer = podId;
    auto suffix = Format("-%v-%v", instanceType, cluster);
    if (!buffer.ChopSuffix(suffix)) {
        return {};
    }

    constexpr char Delimiter = '-';
    auto indexString = buffer.RNextTok(Delimiter);

    int result = 0;
    if (TryIntFromString<16>(indexString, result)) {
        return result;
    }

    return {};
}

int FindNextInstanceId(
    const std::vector<std::string>& instanceNames,
    const std::string& cluster,
    const std::string& instanceType)
{
    std::vector<int> existingIds;
    existingIds.reserve(instanceNames.size());

    for (const auto& instanceName : instanceNames) {
        auto index = GetIndexFromPodId(instanceName, cluster, instanceType);
        if (index && *index > 0) {
            existingIds.push_back(*index);
        }
    }

    // Sort and make unique.
    std::sort(existingIds.begin(), existingIds.end());
    auto last = std::unique(existingIds.begin(), existingIds.end());
    existingIds.resize(std::distance(existingIds.begin(), last));

    if (existingIds.empty()) {
        return 1;
    }

    for (int index = 0; index < std::ssize(existingIds); ++index) {
        if (existingIds[index] != index + 1) {
            return index + 1;
        }
    }

    return existingIds.back() + 1;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
