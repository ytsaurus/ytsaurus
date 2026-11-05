#pragma once

#include "public.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TPayloadMigrationOptions
{
    NTableClient::TTableSchemaPtr PayloadSchema;
    NTableClient::TTableSchemaPtr TargetSchema;
};

////////////////////////////////////////////////////////////////////////////////

TPayload DefaultMigrationFunction(
    const TPayload& payload,
    const TPayloadMigrationOptions& options);

void MigrateMessage(TMessage& message, const TComputationStreamSpecStorage& schemaStorage);
void MigrateMessages(std::vector<TMessage>& messages, const TComputationStreamSpecStorage& schemaStorage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
