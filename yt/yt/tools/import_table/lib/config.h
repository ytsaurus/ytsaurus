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

    //! Maximum weight of a single row in the imported table.
    i64 MaxRowWeight;

    //! Regex for filtering out non-parquet files like successful commit markers in S3 and other unrelated stuff.
    NRe2::TRe2Ptr ParquetFileRegex;

    REGISTER_YSON_STRUCT(TImportConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TImportConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NImporter
