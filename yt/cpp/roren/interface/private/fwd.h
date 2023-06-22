#pragma once

#include <util/generic/ptr.h>
#include <util/generic/yexception.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class IRawCoder;
using IRawCoderPtr = TIntrusivePtr<IRawCoder>;

class IRawTransform;
using IRawTransformPtr = TIntrusivePtr<IRawTransform>;

struct IRawRead;
using IRawReadPtr = TIntrusivePtr<IRawRead>;

struct IRawWrite;
using IRawWritePtr = TIntrusivePtr<IRawWrite>;

class IRawInput;
using IRawInputPtr = TIntrusivePtr<IRawInput>;

class IRawOutput;
using IRawOutputPtr = TIntrusivePtr<IRawOutput>;

class IRawParDo;
using IRawParDoPtr = TIntrusivePtr<IRawParDo>;

class IRawGroupByKey;
using IRawGroupByKeyPtr = TIntrusivePtr<IRawGroupByKey>;

class IRawCombine;
using IRawCombinePtr = TIntrusivePtr<IRawCombine>;

class IRawFlatten;
using IRawFlattenPtr = TIntrusivePtr<IRawFlatten>;

class TRawPipeline;
using TRawPipelinePtr = TIntrusivePtr<TRawPipeline>;

class IRawCoGroupByKey;
using IRawCoGroupByKeyPtr = TIntrusivePtr<IRawCoGroupByKey>;

class TPCollectionNode;
using TPCollectionNodePtr = TIntrusivePtr<TPCollectionNode>;

class TTransformNode;
using TTransformNodePtr = TIntrusivePtr<TTransformNode>;

struct TRowVtable;

class IRawStatefulParDo;
using IRawStatefulParDoPtr = TIntrusivePtr<IRawStatefulParDo>;

class IParDoTree;
using IParDoTreePtr = TIntrusivePtr<IParDoTree>;

class IRawStateStore;
using IRawStateStorePtr = TIntrusivePtr<IRawStateStore>;

class TRawPStateNode;
using TRawPStateNodePtr = TIntrusivePtr<TRawPStateNode>;

class TFnAttributesOps;

#define THROW_NOT_IMPLEMENTED_YET() \
do { \
    ythrow yexception() << "not supported yet " << __FUNCTION__; \
} while (0)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
