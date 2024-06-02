#include "utils.h"

#include <yt/yt/library/program/config.h>

#include <yt/yt/core/logging/config.h>

namespace NYT::NHydra {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void ConfigureDryRunLogging(const TSingletonsConfigPtr& config)
{
    config->Logging = NLogging::TLogManagerConfig::CreateQuiet();

    auto silentRule = New<NLogging::TRuleConfig>();
    silentRule->MinLevel = NLogging::ELogLevel::Debug;
    silentRule->Writers.push_back("dev_null");

    auto fileWriterConfig = New<NLogging::TFileLogWriterConfig>();
    fileWriterConfig->FileName = "/dev/null";

    config->Logging->Rules.push_back(silentRule);
    config->Logging->Writers.emplace("dev_null", ConvertTo<IMapNodePtr>(fileWriterConfig));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
