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
    const std::string& proxy,
    const std::string& url,
    const std::string& region,
    const std::string& bucket,
    const std::string& prefix,
    const std::string& resultTable,
    EFileFormat format,
    const std::optional<std::string>& networkProject = std::nullopt,
    TImportConfigPtr config = New<TImportConfig>());

void ImportFilesFromHuggingface(
    const std::string& proxy,
    const std::string& dataset,
    const std::string& subset,
    const std::string& split,
    const std::string& resultTable,
    EFileFormat format,
    const std::optional<std::string>& networkProject = std::nullopt,
    // TODO(max42): introduce a derived THuggingFaceImportConfig and move next argument there.
    const std::optional<std::string>& urlOverride = std::nullopt,
    TImportConfigPtr config = New<TImportConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NImporter
