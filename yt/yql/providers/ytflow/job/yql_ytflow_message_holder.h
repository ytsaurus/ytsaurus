#pragma once


#include <yt/yt/flow/library/cpp/common/message.h>

#include <util/generic/ptr.h>


namespace NYql::NYtflow {

class TMessageHolder {
public:
    explicit TMessageHolder(NYT::NFlow::TInputMessageConstPtr inputMessage);
    explicit TMessageHolder(THolder<NYT::NFlow::TMessage> transformedMessage);

    const NYT::NFlow::TMessage& GetMessage() const;

private:
    NYT::NFlow::TInputMessageConstPtr InputMessage;
    THolder<NYT::NFlow::TMessage> TransformedMessage;
    bool InputMessageSet;
};

} // namespace NYql::NYtflow

#include "yql_ytflow_message_holder-inl.h"
