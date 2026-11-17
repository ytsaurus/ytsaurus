#pragma once

#ifndef JOB_INIT_CONTEXT_INL_H_
    #error "Direct inclusion of this file is not allowed, include job_init_context.h"
    // For the sake of sane code completion.
    #include "job_init_context.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <CYsonMessage TTask, CYsonMessage TResult>
void IJobInitContext::InitAsyncProcessor(
    TAsyncProcessorHandle<TTask, TResult>& handle,
    TStringBuf name,
    typename TAsyncProcessorHandle<TTask, TResult>::TStart start,
    typename TAsyncProcessorHandle<TTask, TResult>::TResultHandler resultHandler) const
{
    handle = TAsyncProcessorHandle<TTask, TResult>(name, std::move(start), std::move(resultHandler));
    RegisterAsyncProcessor(handle.GetDescriptor());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
