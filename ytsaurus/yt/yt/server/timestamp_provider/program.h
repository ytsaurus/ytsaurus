#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

class TTimestampProviderProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TTimestampProviderConfig>
{
public:
    TTimestampProviderProgram();

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
