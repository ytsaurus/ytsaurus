#ifndef TRACE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include trace_context.h"
#endif
#undef TRACE_CONTEXT_INL_H_

namespace NYT {
namespace NTracing {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TraceEvent(
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationKey,
    const T& annotationValue)
{
    TraceEvent(
        serviceName,
        spanName,
        annotationKey,
        ToString(annotationValue));
}

template <class T>
void TraceEvent(
    const TTraceContext& context,
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationKey,
    const T& annotationValue)
{
    TraceEvent(
        context,
        serviceName,
        spanName,
        annotationKey,
        ToString(annotationValue));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT
