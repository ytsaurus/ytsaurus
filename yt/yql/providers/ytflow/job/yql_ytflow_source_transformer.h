#pragma once

#include "yql_ytflow_message_holder.h"

#include <yt/yt/flow/library/cpp/common/message.h>


namespace NYql::NYtflow {

DEFINE_ENUM(ESourceType,
    ((YT)        (0))
    ((Logbroker) (1))
);

struct ISourceTransformer {
    virtual TMessageHolder Transform(
        const NYT::NFlow::TInputMessageConstPtr& message,
        NYT::NTableClient::TTableSchemaPtr targetSchema) = 0;

    virtual ~ISourceTransformer() = default;
};

THolder<ISourceTransformer> CreateSourceTransformer(ESourceType sourceType);

} // namespace NYql::NYtflow
