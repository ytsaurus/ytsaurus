#pragma once

#include "public.h"

#include <yt/yt/library/program/public.h>

#include <yt/yt/library/re2/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTools::NImporter {

////////////////////////////////////////////////////////////////////////////////

struct TImportConfig
    : public NYTree::TYsonStruct
{
    //! Singletons configuration to be used in the main importer process.
    TSingletonsConfigPtr Singletons;
    //! Singletons configuration to be used in the map-reduce jobs.
    TSingletonsConfigPtr JobSingletons;

    //! Scheduling pool for YT operations.
    std::optional<TString> Pool;

    //! Memory limit for download operation.
    i64 MemoryLimit;

    //! Maximum weight of a single row in the imported table.
    i64 MaxRowWeight;

    //! Maximum weight of a single row in the metadata table.
    i64 MaxMetadataRowWeight;

    //! Regex for filtering out non-parquet files like successful commit markers in S3 and other unrelated stuff.
    NRe2::TRe2Ptr ParquetFileRegex;

    //! Regex for filtering out non-orc files like successful commit markers in S3 and other unrelated stuff.
    NRe2::TRe2Ptr OrcFileRegex;

    REGISTER_YSON_STRUCT(TImportConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TImportConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NImporter
