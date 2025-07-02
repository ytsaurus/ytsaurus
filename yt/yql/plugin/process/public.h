#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NYqlPlugin {
namespace NProcess {

extern const TString YqlAgentProgrammName;
extern const TString YqlPluginSubcommandName;
extern const TString YqlPluginConfigFilename;

DECLARE_REFCOUNTED_STRUCT(TYqlProcessPluginConfig)

} // namespace NProcess
} // namespace NYT::NYqlPlugin
