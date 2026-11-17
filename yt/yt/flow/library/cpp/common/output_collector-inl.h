#pragma once

#ifndef OUTPUT_COLLECTOR_INL_H_
    #error "Direct inclusion of this file is not allowed, include output_collector.h"
    // For the sake of sane code completion.
    #include "output_collector.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <CYsonMessage TTask, CYsonMessage TResult>
void IOutputCollector::Add(
    const TAsyncProcessorHandle<TTask, TResult>& handle,
    std::type_identity_t<TIntrusivePtr<TTask>> task)
{
    DoAddTask(handle.GetDescriptor(), std::move(task));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
