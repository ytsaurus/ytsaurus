#pragma once

#include "yql_ytflow_message_holder.h"


namespace NYql::NYtflow {

inline TMessageHolder::TMessageHolder(NYT::NFlow::TInputMessageConstPtr inputMessage)
    : InputMessage(std::move(inputMessage))
    , InputMessageSet(true)
{
}

inline TMessageHolder::TMessageHolder(THolder<NYT::NFlow::TMessage> transformedMessage)
    : TransformedMessage(std::move(transformedMessage))
    , InputMessageSet(false)
{
}

inline const NYT::NFlow::TMessage& TMessageHolder::GetMessage() const {
    if (InputMessageSet) {
        return *InputMessage;
    }

    return *TransformedMessage;
}

} // namespace NYql::NYtflow
