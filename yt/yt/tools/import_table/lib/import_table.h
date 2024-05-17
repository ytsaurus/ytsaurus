#pragma once

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NTools::NImporter {

////////////////////////////////////////////////////////////////////////////////

void ImportParquetFilesFromS3(
    const TString& proxy,
    const TString& url,
    const TString& region,
    const TString& bucket,
    const TString& prefix,
    const TString& resultTable);

void ImportParquetFilesFromHuggingface(
    const TString& proxy,
    const TString& dataset,
    const TString& config,
    const TString& split,
    const TString& resultTable,
    const std::optional<TString>& urlOverride = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // NYT::NTools::NImporter
