#pragma once

#include "public.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TValidatePayloadOptions
{
    bool AllowEmptySchema = false;
    NTableClient::TTableSchemaPtr ExpectedSchema = nullptr;
    bool ValidateValues = true;
};

TError DoValidatePayload(
    const std::string& rowName,
    const NTableClient::TUnversionedRow& row,
    const std::string& schemaName,
    const NTableClient::TTableSchemaPtr& schema,
    const TValidatePayloadOptions& options = {});
TError DoValidatePayload(
    const std::string& rowName,
    const TPayload& payload,
    const std::string& schemaName,
    const NTableClient::TTableSchemaPtr& schema,
    const TValidatePayloadOptions& options = {});
TError DoValidatePayload(
    const std::string& rowName,
    const TKey& key,
    const std::string& schemaName,
    const NTableClient::TTableSchemaPtr& schema,
    const TValidatePayloadOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
