#pragma once

#include <string_view>

namespace NYT::NFlow::NInternalUrls {

////////////////////////////////////////////////////////////////////////////////

inline constexpr std::string_view TvmInfoUrlPrefix = "https://tvm.yandex-team.ru/client/";
inline constexpr std::string_view TracingAddress = "https://monitoring.yandex-team.ru/projects/yt/traces";
inline constexpr std::string_view DeployAddress = "https://deploy.yandex-team.ru";
inline constexpr std::string_view YtprofUrlPrefix = "https://ytprof.yt.yandex-team.ru/api/online_fetch";
inline constexpr std::string_view YtUrlPrefix = "https://yt.yandex-team.ru/";
inline constexpr std::string_view JaegerCollectorAddressSuffix = ".collector.t.yandex-team.ru:14250";
inline constexpr std::string_view VcsCommitUrlPrefix = "https://a.yandex-team.ru/arcadia/commit/";
inline constexpr std::string_view VcsRevisionUrlPrefix = "https://a.yandex-team.ru/arcadia?rev=r";
inline constexpr std::string_view VcsBranchUrlPrefix = "https://a.yandex-team.ru/arcadia?rev=";
inline constexpr std::string_view FlowCoreTargetDocUrl = "https://yt.yandex-team.ru/docs/flow/release/flow-core-target";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NInternalUrls
