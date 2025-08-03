#include "config.h"

#include <yt/yt/library/program/config.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NTools::NImporter {

////////////////////////////////////////////////////////////////////////////////

void TImportConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("singletons", &TThis::Singletons)
        .DefaultNew();
    registrar.Parameter("job_singletons", &TThis::JobSingletons)
        .DefaultNew();
    registrar.Parameter("max_row_weight", &TThis::MaxRowWeight)
        .Default(16_MB);
    registrar.Parameter("parquet_file_regex", &TThis::ParquetFileRegex)
        .DefaultNew("\\.par(quet)?");
    registrar.Parameter("orc_file_regex", &TThis::ORCFileRegex)
        .DefaultNew("\\.orc");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NImporter
