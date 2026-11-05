#pragma once

#include <string_view>

namespace NYT::NFlow::NInternalUrls {

////////////////////////////////////////////////////////////////////////////////

// Defined in internal_urls_yandex.cpp or internal_urls_opensource.cpp,
// selected in ya.make by the OPENSOURCE flag.
extern const std::string_view TvmInfoUrlPrefix;
extern const std::string_view TracingAddress;
extern const std::string_view DeployAddress;
extern const std::string_view YtprofUrlPrefix;
extern const std::string_view YtUrlPrefix;
extern const std::string_view JaegerCollectorAddressSuffix;
extern const std::string_view VcsCommitUrlPrefix;
extern const std::string_view VcsRevisionUrlPrefix;
extern const std::string_view VcsBranchUrlPrefix;
extern const std::string_view FlowCoreTargetDocUrl;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NInternalUrls
