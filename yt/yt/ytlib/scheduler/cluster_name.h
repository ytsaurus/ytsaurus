#pragma once

#include <library/cpp/yt/misc/strong_typedef.h>

#include <util/str_stl.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TClusterName, std::string)

const TClusterName LocalClusterName{};

bool IsLocal(const TClusterName& clusterName);
void FormatValue(TStringBuilderBase* builder, const TClusterName& name, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
