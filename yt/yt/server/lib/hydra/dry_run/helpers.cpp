#include "helpers.h"

#include <yt/yt/core/logging/config.h>

namespace NYT::NHydra {

using namespace NYTree;

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TLogManagerConfigPtr CreateDryRunLoggingConfig(bool abortOnAlert)
{
    auto config = TLogManagerConfig::CreateStderrLogger(ELogLevel::Alert);
    config->AbortOnAlert = abortOnAlert;

    auto silentRule = New<TRuleConfig>();
    silentRule->MinLevel = ELogLevel::Debug;
    silentRule->Writers.push_back("dev_null");

    auto fileWriterConfig = New<TFileLogWriterConfig>();
    fileWriterConfig->FileName = "/dev/null";

    config->Rules.push_back(silentRule);
    config->Writers.emplace("dev_null", ConvertTo<IMapNodePtr>(fileWriterConfig));

    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
