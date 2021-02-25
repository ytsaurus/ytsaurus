#include "bootstrap.h"
#include "config.h"

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/program_setsid_mixin.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TMasterCacheConfig>
{
public:
    TMasterCacheProgram();

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
