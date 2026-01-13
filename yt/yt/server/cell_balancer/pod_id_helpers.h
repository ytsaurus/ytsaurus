#pragma once

#include "public.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

std::string GetPodIdForInstance(const TCypressAnnotationsPtr& cypressAnnotations, const std::string& name);

std::string GetInstancePodIdTemplate(
    const std::string& cluster,
    const std::string& bundleName,
    const std::string& instanceType,
    int index);

std::optional<int> GetIndexFromPodId(
    const std::string& podId,
    const std::string& cluster,
    const std::string& instanceType);

int FindNextInstanceId(
    const std::vector<std::string>& instanceNames,
    const std::string& cluster,
    const std::string& instanceType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
