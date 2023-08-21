#pragma once

#include <util/generic/ptr.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TPCollection;

class TMultiPCollection;

class TPipeline;

class IExecutor;
using IExecutorPtr = ::TIntrusivePtr<IExecutor>;

class TGroupByKey;

struct TMultiRow;

template <typename T>
class TInput;

template <typename T>
class TInputPtr;

using TMultiInput = TInput<TMultiRow>;
using TMultiInputPtr = ::TIntrusivePtr<TMultiInput>;

template <typename T>
class TOutput;

using TMultiOutput = TOutput<TMultiRow>;

template <typename I, typename A, typename O>
class ICombineFn;

template <typename I, typename A, typename O>
using ICombineFnPtr = ICombineFn<I, A, O>;

template <typename T>
class TWriteTransform;

template <typename T>
class TReadTransform;

template <typename I, typename O>
class TParDoTransform;

template <typename T>
class TTypeTag;

template <typename I, typename O>
class TTransform;

class TDynamicTypeTag;

class TCoGbkResult;

template <typename T>
class TCoder;

class IExecutionContext;
using IExecutionContextPtr = ::TIntrusivePtr<IExecutionContext>;

template <typename K, typename V>
class TKV;

template <typename, typename>
class TPState;

template <typename, typename>
class TStateStore;

class TNullWriteTransform;

class TFnAttributes;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren


namespace testing {

template <typename T>
::std::string PrintToString(const T& value);

}
