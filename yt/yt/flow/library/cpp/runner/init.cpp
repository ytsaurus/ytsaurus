#include "init.h"
#include "debug_build_warning.h"
#include "node.h"
#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/system/env.h>

#include <cstdlib>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void Initialize(int argc, const char** argv)
{
    ForbidContextSwitchInFutureHandler();

    MaybeWarnSlowBuild();

    if (std::getenv(FlowModeEnvVarName.data())) {
        // Running as a vanilla job: YT delivers the operation's secure vault as the YT_SECURE_VAULT
        // env var (a YSON map). Re-export each entry as a plain env var so that TVM and connectors
        // find their secrets (e.g. TVM_SECRET).
        auto secureVault = NYTree::ConvertToNode(NYson::TYsonString(GetEnv("YT_SECURE_VAULT", "{}")));
        for (const auto& [key, value] : secureVault->AsMap()->GetChildren()) {
            SetEnv(TString(key), TString(value->AsString()->GetValue()));
        }
        ::exit(RunFlowNode(argc, argv));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
