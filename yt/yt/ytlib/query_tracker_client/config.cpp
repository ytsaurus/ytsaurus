#include "config.h"

namespace NYT::NQueryTrackerClient {

///////////////////////////////////////////////////////////////////////////////

void TQueryTrackerStageConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/query_tracker");
    registrar.Parameter("user", &TThis::User)
        .Default("query_tracker");
}

///////////////////////////////////////////////////////////////////////////////

void TQueryTrackerConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stages", &TThis::Stages)
        .Default();
    registrar.Parameter("max_query_file_count", &TThis::MaxQueryFileCount)
        .Default(8192);
    registrar.Parameter("max_query_file_name_size_bytes", &TThis::MaxQueryFileNameSizeBytes)
        .Default(1_KB);
    registrar.Parameter("max_query_file_content_size_bytes", &TThis::MaxQueryFileContentSizeBytes)
        .Default(2_KB);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
