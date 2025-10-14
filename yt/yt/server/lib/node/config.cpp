#include "config.h"

namespace NYT::NNode {

using namespace NServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TChunkLocationConfigBase::ApplyDynamicInplace(const TChunkLocationDynamicConfigBase& dynamicConfig)
{
    TDiskLocationConfig::ApplyDynamicInplace(dynamicConfig);

    UpdateYsonStructField(IOEngineType, dynamicConfig.IOEngineType);
    UpdateYsonStructField(IOConfig, dynamicConfig.IOConfig);

    UpdateYsonStructField(DiskHealthChecker, DiskHealthChecker->ApplyDynamic(*dynamicConfig.DiskHealthChecker));

    UpdateYsonStructField(LegacyWriteMemoryLimit, dynamicConfig.LegacyWriteMemoryLimit);

    UpdateYsonStructField(ReadMemoryLimit, dynamicConfig.ReadMemoryLimit);

    UpdateYsonStructField(TotalMemoryLimit, dynamicConfig.TotalMemoryLimit);

    UpdateYsonStructField(WriteMemoryLimit, dynamicConfig.WriteMemoryLimit);

    UpdateYsonStructField(SessionCountLimit, dynamicConfig.SessionCountLimit);
}

void TChunkLocationConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("quota", &TThis::Quota)
        .GreaterThanOrEqual(0)
        .Default();

    registrar.Parameter("disk_health_checker", &TThis::DiskHealthChecker)
        .DefaultNew();

    registrar.Parameter("io_engine_type", &TThis::IOEngineType)
        .Default(NIO::EIOEngineType::ThreadPool);
    registrar.Parameter("io_config", &TThis::IOConfig)
        .Optional();

    registrar.Parameter("read_memory_limit", &TThis::ReadMemoryLimit)
        .Default(10_GB);
    registrar.Parameter("write_memory_limit", &TThis::WriteMemoryLimit)
        .Default(10_GB);
    registrar.Parameter("total_memory_limit", &TThis::TotalMemoryLimit)
        .Default(20_GB);

    registrar.Parameter("session_count_limit", &TThis::SessionCountLimit)
        .Default(1000);

    registrar.Parameter("reset_uuid", &TThis::ResetUuid)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        config->LegacyWriteMemoryLimit = config->WriteMemoryLimit;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TChunkLocationDynamicConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("io_engine_type", &TThis::IOEngineType)
        .Optional();
    registrar.Parameter("io_config", &TThis::IOConfig)
        .Optional();

    registrar.Parameter("disk_health_checker", &TThis::DiskHealthChecker)
        .DefaultNew();

    registrar.Parameter("read_memory_limit", &TThis::ReadMemoryLimit)
        .Optional();
    registrar.Parameter("write_memory_limit", &TThis::WriteMemoryLimit)
        .Optional();
    registrar.Parameter("total_memory_limit", &TThis::TotalMemoryLimit)
        .Optional();

    registrar.Parameter("session_count_limit", &TThis::SessionCountLimit)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        config->LegacyWriteMemoryLimit = config->WriteMemoryLimit;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNode
