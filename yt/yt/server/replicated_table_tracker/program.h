#pragma once

#include "config.h"

#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TReplicatedTableTrackerServerConfig>
{
public:
    TReplicatedTableTrackerProgram();

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
