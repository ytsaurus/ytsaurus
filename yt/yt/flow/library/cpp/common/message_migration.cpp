#include "message_migration.h"

#include "message.h"
#include "registry.h"
#include "spec.h"
#include "stream_spec_storage.h"

namespace NYT::NFlow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TPayload DefaultMigrationFunction(
    const TPayload& payload,
    const TPayloadMigrationOptions& options)
{
    const auto& payloadSchema = options.PayloadSchema;
    const auto& targetSchema = options.TargetSchema;

    YT_VERIFY(payloadSchema);
    YT_VERIFY(targetSchema);

    TPayloadBuilder builder(targetSchema);

    for (const auto& targetColumn : targetSchema->Columns()) {
        auto value = MakeUnversionedNullValue();

        if (const auto* payloadColumn = payloadSchema->FindColumn(targetColumn.Name())) {
            if (targetColumn.GetWireType() == payloadColumn->GetWireType()) {
                value = payload.Underlying()[payloadSchema->GetColumnIndex(*payloadColumn)];
            }
        }

        if (value.Type == EValueType::Null) {
            THROW_ERROR_EXCEPTION_IF(targetColumn.Required(),
                "Required column %Qv cannot have null value",
                targetColumn.Name());
        }

        builder.SetValue(value, targetSchema->GetColumnIndex(targetColumn));
    }

    return builder.Finish();
}

void MigrateMessage(TMessage& message, const TComputationStreamSpecStorage& specStorage)
{
    const auto lastSpecId = specStorage.GetLastSpecId(message.StreamId);
    const auto targetSpec = specStorage.GetSpec(lastSpecId);

    if (targetSpec->Schema == message.PayloadSchema) {
        return;
    }

    const auto migrationFunc = TRegistry::Get()->GetPayloadMigrationFunction(
        targetSpec->MigrationFunction);

    const TPayloadMigrationOptions options{
        .PayloadSchema = message.PayloadSchema,
        .TargetSchema = targetSpec->Schema,
    };

    message.Payload = migrationFunc(message.Payload, options);
    message.PayloadSchema = targetSpec->Schema;

    ValidateMessage(message);
}

void MigrateMessages(std::vector<TMessage>& messages, const TComputationStreamSpecStorage& specStorage)
{
    for (auto& message : messages) {
        MigrateMessage(message, specStorage);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

YT_FLOW_DEFINE_PAYLOAD_MIGRATION_FUNCTION(NYT::NFlow::DefaultMigrationFunction);
