#include "helpers.h"

#include <yt/yt/core/logging/config.h>

namespace NYT::NHydra {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogManagerConfigPtr CreateDryRunLoggingConfig()
{
    auto config = NLogging::TLogManagerConfig::CreateQuiet();

    auto silentRule = New<NLogging::TRuleConfig>();
    silentRule->MinLevel = NLogging::ELogLevel::Debug;
    silentRule->Writers.push_back("dev_null");

    auto fileWriterConfig = New<NLogging::TFileLogWriterConfig>();
    fileWriterConfig->FileName = "/dev/null";

    config->Rules.push_back(silentRule);
    config->Writers.emplace("dev_null", ConvertTo<IMapNodePtr>(fileWriterConfig));

    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
