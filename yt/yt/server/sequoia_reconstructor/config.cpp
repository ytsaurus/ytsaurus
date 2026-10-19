#include "config.h"

#include <yt/yt/core/ytree/node.h>

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

void TFileOutputConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("file_name", &TThis::FileName);

    registrar.Parameter("text_yson_output_format", &TThis::TextYsonOutputFormat)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TSingleCellSequoiaReconstructorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("master_config_path", &TThis::MasterConfigPath);

    registrar.Parameter("snapshot_path", &TThis::SnapshotPath);

    registrar.Parameter("tables_output", &TThis::TablesOutput)
        .NonEmpty();

    registrar.Postprocessor([] (TThis* config) {
        if (config->TablesOutput.contains(EOutputTableType::PathToNodeChanges)) {
            THROW_ERROR_EXCEPTION("path_to_node_changes is not supported for single cell sequoia reconstructor");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TCellConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_tag", &TThis::CellTag);
    registrar.Parameter("config_ypath", &TThis::ConfigYPath);
    registrar.Parameter("snapshot_ypath", &TThis::SnapshotYPath);
}

////////////////////////////////////////////////////////////////////////////////

void TMulticellMapReduceSequoiaReconstructorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_name", &TThis::ClusterName);

    registrar.Parameter("rpc_connection_config", &TThis::RpcConnectionConfig)
        .Default();

    registrar.Parameter("pool", &TThis::Pool)
        .Default();

    registrar.Parameter("cells", &TThis::Cells)
        .NonEmpty();

    registrar.Parameter("workdir_ypath", &TThis::WorkdirYPath);
    registrar.Parameter("table_paths", &TThis::TablePaths)
        .NonEmpty();

    registrar.Parameter("mapper_job_count", &TThis::MapperJobCount)
        .Default(10)
        .GreaterThan(0);
    registrar.Parameter("mapper_cpu_limit", &TThis::MapperCpuLimit)
        .Default(1.0);
    registrar.Parameter("mapper_time_limit", &TThis::MapperTimeLimit)
        .Default(TDuration::Minutes(90));
    registrar.Parameter("mapper_memory_limit_gb", &TThis::MapperMemoryLimitGb)
        .Default(3.0);
    registrar.Parameter("max_failed_job_count", &TThis::MaxFailedJobCount)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

void TYtMapJobConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("master_config_file", &TThis::MasterConfigFile);
    registrar.Parameter("snapshot_file", &TThis::SnapshotFile);

    registrar.Parameter("job_count", &TThis::JobCount);
    registrar.Parameter("job_index", &TThis::JobIndex)
        .GreaterThanOrEqual(0);

    registrar.Parameter("table_indexes", &TThis::TableIndexes)
        .NonEmpty();

    registrar.Postprocessor([] (TThis* config) {
        if (config->JobIndex >= config->JobCount) {
            THROW_ERROR_EXCEPTION("Job index %v must be less than job count %v",
                config->JobIndex,
                config->JobCount);
        }

        if (config->TableIndexes.contains(EOutputTableType::PathForks)) {
            THROW_ERROR_EXCEPTION("path_forks should not be created in map operation");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
