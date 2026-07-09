#pragma once

#include <string_view>

#ifdef YT_INTERNAL_FLOW
    #include <yt/yt/flow/yandex/library/cpp/internal_urls/internal_urls.h>
#else

namespace NYT::NFlow::NInternalUrls {

////////////////////////////////////////////////////////////////////////////////

inline constexpr std::string_view TvmInfoUrlPrefix = "";
inline constexpr std::string_view TracingAddress = "";
inline constexpr std::string_view DeployAddress = "";
inline constexpr std::string_view YtprofUrlPrefix = "";
inline constexpr std::string_view YtUrlPrefix = "";
inline constexpr std::string_view JaegerCollectorAddressSuffix = "";
inline constexpr std::string_view VcsCommitUrlPrefix = "";
inline constexpr std::string_view VcsRevisionUrlPrefix = "";
inline constexpr std::string_view VcsBranchUrlPrefix = "";
inline constexpr std::string_view FlowCoreTargetDocUrl = "";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NInternalUrls

#endif
