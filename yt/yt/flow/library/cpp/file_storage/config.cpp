#include "config.h"

#include <util/folder/path.h>

namespace NYT::NFlow::NFileStorage {

////////////////////////////////////////////////////////////////////////////////

void TFileStorageConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .NonEmpty();
    registrar.Parameter("soft_size_limit", &TThis::SoftSizeLimit)
        .GreaterThan(0);
    registrar.Parameter("hard_size_limit", &TThis::HardSizeLimit)
        .GreaterThan(0);
    registrar.Parameter("cleanup_period", &TThis::CleanupPeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Minutes(1));

    registrar.Postprocessor([] (TThis* config) {
        THROW_ERROR_EXCEPTION_UNLESS(
            TFsPath(config->Path).IsAbsolute(),
            "File storage path %Qv must be absolute",
            config->Path);
        THROW_ERROR_EXCEPTION_UNLESS(
            config->SoftSizeLimit <= config->HardSizeLimit,
            "File storage soft size limit must not exceed hard size limit")
            .With("soft_size_limit", config->SoftSizeLimit)
            .With("hard_size_limit", config->HardSizeLimit);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileStorage
