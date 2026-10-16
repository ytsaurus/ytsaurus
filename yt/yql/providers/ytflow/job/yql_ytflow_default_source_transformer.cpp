#include "yql_ytflow_default_source_transformer.h"

namespace NYql::NYtflow {

class TDefaultSourceTransformer final : public ISourceTransformer {
    TMessageHolder Transform(
        const NYT::NFlow::TInputMessageConstPtr& message,
        NYT::NTableClient::TTableSchemaPtr /*targetSchema*/)
    {
        auto messageHolder = TMessageHolder(message);
        return messageHolder;
    }
};

THolder<ISourceTransformer> CreateDefaultSourceTransformer() {
    return MakeHolder<TDefaultSourceTransformer>();
}

} // namespace NYql::NYtflow
