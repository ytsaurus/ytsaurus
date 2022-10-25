#include "schema_inferer.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Checks that all input tables respect output table schema.
class TSchemaCompatibilityChecker
    : public IOutputSchemaInferer
{
public:
    TSchemaCompatibilityChecker(const NYPath::TYPath& outputPath, const TTableSchemaPtr& outputTableSchema)
        : OutputPath_(outputPath)
        , OutputTableSchema_(outputTableSchema)
    { }

    void AddInputTableSchema(const NYPath::TYPath& path, const TTableSchema& tableSchema, ETableSchemaMode /*schemaMode*/) override
    {
        const auto& [compatibility, error] = CheckTableSchemaCompatibility(tableSchema, *OutputTableSchema_, /*ignoreSortOrder*/ true);
        if (compatibility != ESchemaCompatibility::FullyCompatible) {
            THROW_ERROR_EXCEPTION("Schema of output table %v is not compatible with schema of input table %v",
                OutputPath_,
                path)
                << error;
        }
    }

    const TTableSchemaPtr& GetOutputTableSchema() const override
    {
        return OutputTableSchema_;
    }

    ETableSchemaMode GetOutputTableSchemaMode() const override
    {
        return ETableSchemaMode::Strong;
    }

private:
    const NYPath::TYPath OutputPath_;
    const TTableSchemaPtr OutputTableSchema_;
};

////////////////////////////////////////////////////////////////////////////////

//! Tries to derive output table schema from input tables.
class TOutputSchemaInferer
    : public IOutputSchemaInferer
{
public:
    void AddInputTableSchema(const NYPath::TYPath& /*path*/, const TTableSchema& tableSchema, ETableSchemaMode schemaMode) override
    {
        if (!AddedInputTables_) {
            if (schemaMode == ETableSchemaMode::Weak) {
                OutputTableSchema_ = New<TTableSchema>();
            } else {
                OutputTableSchema_ = tableSchema.ToStrippedColumnAttributes()->ToCanonical();
            }
            OutputTableSchemaMode_ = schemaMode;
            AddedInputTables_ = true;
        } else {
            if (schemaMode == ETableSchemaMode::Weak) {
                OutputTableSchemaMode_ = ETableSchemaMode::Weak;
            }
            if (*OutputTableSchema_ != *tableSchema.ToStrippedColumnAttributes()->ToCanonical()) {
                OutputTableSchema_ = New<TTableSchema>();
                OutputTableSchemaMode_ = ETableSchemaMode::Weak;
            }
        }
    }

    const TTableSchemaPtr& GetOutputTableSchema() const override
    {
        return OutputTableSchema_;
    }

    ETableSchemaMode GetOutputTableSchemaMode() const override
    {
        return OutputTableSchemaMode_;
    }

private:
    TTableSchemaPtr OutputTableSchema_ = New<TTableSchema>();
    ETableSchemaMode OutputTableSchemaMode_ = ETableSchemaMode::Weak;
    bool AddedInputTables_ = false;
};

////////////////////////////////////////////////////////////////////////////////


std::unique_ptr<IOutputSchemaInferer> CreateSchemaCompatibilityChecker(
    const NYPath::TYPath& outputPath,
    const TTableSchemaPtr& outputTableSchema)
{
    return std::make_unique<TSchemaCompatibilityChecker>(outputPath, outputTableSchema);
}

std::unique_ptr<IOutputSchemaInferer> CreateOutputSchemaInferer()
{
    return std::make_unique<TOutputSchemaInferer>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
