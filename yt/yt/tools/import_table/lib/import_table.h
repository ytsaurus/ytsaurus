#pragma once

#include "config.h"

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NTools::NImporter {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFileFormat,
    (Orc)
    (Parquet)
);

////////////////////////////////////////////////////////////////////////////////

void ImportFilesFromS3(
    const TString& proxy,
    const std::string& url,
    const TString& region,
    const TString& bucket,
    const TString& prefix,
    const TString& resultTable,
    EFileFormat format,
    const std::optional<TString>& networkProject = std::nullopt,
    TImportConfigPtr config = New<TImportConfig>());

void ImportFilesFromHuggingface(
    const TString& proxy,
    const TString& dataset,
    const TString& subset,
    const TString& split,
    const TString& resultTable,
    EFileFormat format,
    const std::optional<TString>& networkProject = std::nullopt,
    // TODO(max42): introduce a derived THuggingFaceImportConfig and move next argument there.
    const std::optional<TString>& urlOverride = std::nullopt,
    TImportConfigPtr config = New<TImportConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NImporter
