#pragma once

#include "public.h"

#include <yt/core/ypath/public.h>

#include <memory>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IOutputSchemaInferer
{
    virtual ~IOutputSchemaInferer() = default;
    virtual void AddInputTableSchema(const NYPath::TYPath& path, const TTableSchema& tableSchema, ETableSchemaMode schemaMode) = 0;
    virtual const TTableSchema& GetOutputTableSchema() const = 0;
    virtual ETableSchemaMode GetOutputTableSchemaMode() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Creates IOutputSchemaInferer that checks that all input tables respect output table schema.
std::unique_ptr<IOutputSchemaInferer> CreateSchemaCompatibilityChecker(const NYPath::TYPath& outputPath, const TTableSchema& outputTableSchema);

//! Creates IOutputSchemaInferer that tries to derive output table schema from input tables.
std::unique_ptr<IOutputSchemaInferer> CreateOutputSchemaInferer();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
