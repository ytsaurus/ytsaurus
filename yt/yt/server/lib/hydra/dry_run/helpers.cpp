#include "helpers.h"

#include <yt/yt/core/logging/config.h>

namespace NYT::NHydra {

using namespace NYTree;

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static const std::unordered_map<std::string, ELogLevel> DryRunMessageLevelOverrides = {
    {"Error updating latest timestamp", ELogLevel::Debug},
    {"Error generating fresh timestamps", ELogLevel::Debug},
};

TLogManagerConfigPtr CreateDryRunLoggingConfig(bool abortOnAlert)
{
    auto config = TLogManagerConfig::CreateStderrLogger(ELogLevel::Alert);
    config->AbortOnAlert = abortOnAlert;
    for (const auto& [message, level] : DryRunMessageLevelOverrides) {
        config->MessageLevelOverrides[message] = level;
    }

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
