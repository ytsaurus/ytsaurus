#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename, typename>
struct TIsRowCoderRequired;

DECLARE_REFCOUNTED_CLASS(IRawCoder);
DECLARE_REFCOUNTED_CLASS(IRawTransform);
DECLARE_REFCOUNTED_STRUCT(IRawRead);
DECLARE_REFCOUNTED_STRUCT(IRawWrite);
DECLARE_REFCOUNTED_CLASS(IRawInput);
DECLARE_REFCOUNTED_CLASS(IRawOutput);
DECLARE_REFCOUNTED_CLASS(IRawParDo);
DECLARE_REFCOUNTED_CLASS(IRawGroupByKey);
DECLARE_REFCOUNTED_CLASS(IRawCombine);
DECLARE_REFCOUNTED_CLASS(IRawFlatten);
DECLARE_REFCOUNTED_CLASS(TRawPipeline);
DECLARE_REFCOUNTED_CLASS(IRawCoGroupByKey);
DECLARE_REFCOUNTED_CLASS(TPCollectionNode);
DECLARE_REFCOUNTED_CLASS(TTransformNode);

struct TRowVtable;

DECLARE_REFCOUNTED_CLASS(IRawStatefulParDo);
DECLARE_REFCOUNTED_CLASS(IRawStatefulTimerParDo);
DECLARE_REFCOUNTED_CLASS(IParDoTree);
DECLARE_REFCOUNTED_CLASS(IRawStateStore);
DECLARE_REFCOUNTED_CLASS(TRawPStateNode);

class TAttributes;
class TFnAttributesOps;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
