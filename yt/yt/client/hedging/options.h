#pragma once

#include <yt/yt/client/api/connection.h>

namespace NYT::NHedgingClient {

//! fill client options from environment variable (client options is permanent for whole lifecycle of program)
// UserName is extracted from YT_USER env variable or uses current system username
// Token is extracted from YT_TOKEN env variable or from file ~/.yt/token
NYT::NApi::TClientOptions GetClientOpsFromEnv();

//! resolves options only once per launch and then returns the cached result
const NYT::NApi::TClientOptions& GetClientOpsFromEnvStatic();

} // namespace NYT::NHedgingClient
