#include "yql_ytflow_logbroker_source_transformer.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NYtflow {

class TLogbrokerSourceTransformer final : public ISourceTransformer {
    TMessageHolder Transform(
        const NYT::NFlow::TInputMessageConstPtr& message,
        NYT::NTableClient::TTableSchemaPtr targetSchema)
    {
        YQL_ENSURE(targetSchema->GetColumnCount() == 1 &&
            targetSchema->Columns()[0].Name() == "Data");

        auto sourceSchema = message->PayloadSchema;
        NYT::NTableClient::TUnversionedRowBuilder builder(targetSchema->GetColumnCount());

        auto sourceDataColumn = sourceSchema->FindColumn("data");
        int columnId = sourceSchema->GetColumnIndex(*sourceDataColumn);
        auto value = message->Payload.Underlying()[columnId];
        value.Id = 0;
        builder.AddValue(value);
        auto row = builder.GetRow();

        auto transformedMessage = MakeHolder<NYT::NFlow::TMessage>();
        // copy only meta as payload will be rewritten
        static_cast<NYT::NFlow::TMessageMeta&>(*transformedMessage) =
            static_cast<const NYT::NFlow::TMessageMeta&>(*message);

        transformedMessage->Payload = NYT::NFlow::TPayload(
            NYT::NFlow::TCompactUnversionedOwningRow(row));
        transformedMessage->PayloadSchema = std::move(targetSchema);

        auto messageHolder = TMessageHolder(std::move(transformedMessage));

        return messageHolder;
    }
};

THolder<ISourceTransformer> CreateLogbrokerSourceTransformer() {
    return MakeHolder<TLogbrokerSourceTransformer>();
}

} // namespace NYql::NYtflow
