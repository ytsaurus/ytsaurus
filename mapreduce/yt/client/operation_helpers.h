#pragma once

#include <mapreduce/yt/common/fwd.h>
#include <mapreduce/yt/interface/fwd.h>

#include <mapreduce/yt/http/fwd.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

ui64 RoundUpFileSize(ui64 size);

bool UseLocalModeOptimization(const TClientContext& context, const IClientRetryPolicyPtr& clientRetryPolicy);

TString GetOperationWebInterfaceUrl(TStringBuf serverName, TOperationId operationId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
