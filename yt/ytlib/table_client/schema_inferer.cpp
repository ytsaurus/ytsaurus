#include "schema_inferer.h"

#include <yt/client/table_client/schema.h>

#include <yt/ytlib/table_client/schema.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Checks that all input tables respect output table schema.
class TSchemaCompatibilityChecker
    : public IOutputSchemaInferer
{
public:
    TSchemaCompatibilityChecker(const NYPath::TYPath& outputPath, const TTableSchema& outputTableSchema)
        : OutputPath_(outputPath)
        , OutputTableSchema_(outputTableSchema)
    { }

    virtual void AddInputTableSchema(const NYPath::TYPath& path, const TTableSchema& tableSchema, ETableSchemaMode /*schemaMode*/) override
    {
        auto res = ValidateTableSchemaCompatibility(tableSchema, OutputTableSchema_, /*ignoreSortOrder*/ true);
        THROW_ERROR_EXCEPTION_IF_FAILED(res, "Schema of output table %v is not compatible with schema of input table %v",
            OutputPath_,
            path);
    }

    virtual const TTableSchema& GetOutputTableSchema() const override
    {
        return OutputTableSchema_;
    }

    virtual ETableSchemaMode GetOutputTableSchemaMode() const override
    {
        return ETableSchemaMode::Strong;
    }

private:
    const NYPath::TYPath OutputPath_;
    const TTableSchema OutputTableSchema_;
};

////////////////////////////////////////////////////////////////////////////////

//! Tries to derive output table schema from input tables.
class TOutputSchemaInferer
    : public IOutputSchemaInferer
{
public:
    virtual void AddInputTableSchema(const NYPath::TYPath& /*path*/, const TTableSchema& tableSchema, ETableSchemaMode schemaMode) override
    {
        if (!OutputTableSchema_) {
            if (schemaMode == ETableSchemaMode::Weak) {
                OutputTableSchema_ = TTableSchema();
            } else {
                OutputTableSchema_ = tableSchema.ToStrippedColumnAttributes().ToCanonical();
            }
            OutputTableSchemaMode_ = schemaMode;
        } else {
            if (schemaMode == ETableSchemaMode::Weak) {
                OutputTableSchemaMode_ = ETableSchemaMode::Weak;
            }
            if (*OutputTableSchema_ != tableSchema.ToStrippedColumnAttributes().ToCanonical()) {
                OutputTableSchema_ = TTableSchema();
                OutputTableSchemaMode_ = ETableSchemaMode::Weak;
            }
        }
    }

    virtual const TTableSchema& GetOutputTableSchema() const override
    {
        YCHECK(OutputTableSchema_);
        return *OutputTableSchema_;
    }

    virtual ETableSchemaMode GetOutputTableSchemaMode() const override
    {
        YCHECK(OutputTableSchemaMode_);
        return *OutputTableSchemaMode_;
    }

private:
    const TTableSchema EmptySchema_;
    TNullable<TTableSchema> OutputTableSchema_;
    TNullable<ETableSchemaMode> OutputTableSchemaMode_;
};

////////////////////////////////////////////////////////////////////////////////


std::unique_ptr<IOutputSchemaInferer> CreateSchemaCompatibilityChecker(const NYPath::TYPath& outputPath, const TTableSchema& outputTableSchema)
{
    return std::make_unique<TSchemaCompatibilityChecker>(outputPath, outputTableSchema);
}

std::unique_ptr<IOutputSchemaInferer> CreateOutputSchemaInferer()
{
    return std::make_unique<TOutputSchemaInferer>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
