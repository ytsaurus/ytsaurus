#include "utils.h"

#include <yt/yt/library/program/config.h>

#include <yt/yt/core/logging/config.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

void ConfigureDryRunLogging(const TSingletonsConfigPtr& config)
{
    config->Logging = NLogging::TLogManagerConfig::CreateQuiet();

    auto silentRule = New<NLogging::TRuleConfig>();
    silentRule->MinLevel = NLogging::ELogLevel::Debug;
    silentRule->Writers.push_back(TString("dev_null"));

    auto writerConfig = New<NLogging::TLogWriterConfig>();
    writerConfig->Type = NLogging::TFileLogWriterConfig::Type;

    auto fileWriterConfig = New<NLogging::TFileLogWriterConfig>();
    fileWriterConfig->FileName = "/dev/null";

    config->Logging->Rules.push_back(silentRule);
    config->Logging->Writers.emplace(TString("dev_null"), writerConfig->BuildFullConfig(fileWriterConfig));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
