#ifndef TRACE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include trace_context.h"
#endif
#undef TRACE_CONTEXT_INL_H_

namespace NYT {
namespace NTracing {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TraceEvent(
    const Stroka& annotationKey,
    const T& annotationValue)
{
    using ::ToString;
    TraceEvent(
        annotationKey,
        ToString(annotationValue));
}

template <class T>
void TraceEvent(
    const TTraceContext& context,
    const Stroka& annotationKey,
    const T& annotationValue)
{
    using ::ToString;
    TraceEvent(
        context,
        annotationKey,
        ToString(annotationValue));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT
