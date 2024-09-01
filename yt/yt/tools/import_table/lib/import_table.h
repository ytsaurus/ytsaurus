#pragma once

#include "config.h"

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NTools::NImporter {

////////////////////////////////////////////////////////////////////////////////

void ImportParquetFilesFromS3(
    const TString& proxy,
    const TString& url,
    const TString& region,
    const TString& bucket,
    const TString& prefix,
    const TString& resultTable,
    TImportConfigPtr config = New<TImportConfig>());

void ImportParquetFilesFromHuggingface(
    const TString& proxy,
    const TString& dataset,
    const TString& subset,
    const TString& split,
    const TString& resultTable,
    // TODO(max42): introduce a derived THuggingFaceImportConfig and move next argument there.
    const std::optional<TString>& urlOverride = std::nullopt,
    TImportConfigPtr config = New<TImportConfig>());

////////////////////////////////////////////////////////////////////////////////

} // NYT::NTools::NImporter
