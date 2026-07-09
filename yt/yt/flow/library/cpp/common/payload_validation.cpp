#include "payload_validation.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TError DoValidatePayload(
    const std::string& rowName,
    const NTableClient::TUnversionedRow& row,
    const std::string& schemaName,
    const NTableClient::TTableSchemaPtr& schema,
    const TValidatePayloadOptions& options)
{
    if (!row) {
        return TError("%v is undefined", rowName);
    }

    const bool allowEmptySchema = options.AllowEmptySchema && !options.ExpectedSchema;
    if (!schema) {
        if (!allowEmptySchema) {
            return TError("%v is undefined", schemaName);
        }
    } else {
        if (options.ExpectedSchema && schema.Get() != options.ExpectedSchema.Get() && *schema != *options.ExpectedSchema) {
            return TError("%v has unexpected value", schemaName)
                << TErrorAttribute("schema", *schema)
                << TErrorAttribute("expected_schema", *options.ExpectedSchema);
        }
        if (schema->GetColumnCount() != (int)row.GetCount()) {
            return TError("%v and %v count mismatch", rowName, schemaName)
                << TErrorAttribute("row_column_count", row.GetCount())
                << TErrorAttribute("schema_column_count", schema->GetColumnCount());
        }
    }

    for (ui32 index = 0; index < row.GetCount(); ++index) {
        const auto& value = row[index];
        if (index != value.Id) {
            return TError("Value id and index mismatch in %v", rowName)
                << TErrorAttribute("value_id", static_cast<int>(value.Id))
                << TErrorAttribute("value_index", index);
        }
        if (schema && index >= schema->Columns().size()) {
            return TError("Value index is out of bounds of %v", schemaName);
        }
        try {
            if (schema) {
                ValidateValueType(value, *schema, index, /*typeAnyAcceptsAllValues*/ false);
            }
            if (options.ValidateValues) {
                ValidateDataValue(value);
            }
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }
    return TError();
}

TError DoValidatePayload(
    const std::string& rowName,
    const TPayload& payload,
    const std::string& schemaName,
    const NTableClient::TTableSchemaPtr& schema,
    const TValidatePayloadOptions& options)
{
    return DoValidatePayload(rowName, payload.Underlying(), schemaName, schema, options);
}

TError DoValidatePayload(
    const std::string& rowName,
    const TKey& key,
    const std::string& schemaName,
    const NTableClient::TTableSchemaPtr& schema,
    const TValidatePayloadOptions& options)
{
    return DoValidatePayload(rowName, key.Underlying(), schemaName, schema, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
