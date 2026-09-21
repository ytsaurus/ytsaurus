#pragma once

#ifndef FLOW_EXECUTE_INL_H_
    #error "Direct inclusion of this file is not allowed, include flow_execute.h"
    // For the sake of sane code completion.
    #include "flow_execute.h"
#endif

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TArg>
typename TFlowExecuteTraits<TArg>::TResult FlowExecute(
    const TFlowExecuteTarget& target,
    const NYPath::TYPath& pipelinePath,
    const TArg& argument,
    const NApi::TFlowExecuteOptions& options)
{
    auto result = FlowExecute(
        target,
        pipelinePath,
        std::string(TFlowExecuteTraits<TArg>::Command),
        NYson::ConvertToYsonString(argument),
        options);
    return NYTree::ConvertTo<typename TFlowExecuteTraits<TArg>::TResult>(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
