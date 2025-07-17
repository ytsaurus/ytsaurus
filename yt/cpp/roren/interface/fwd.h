#pragma once

#include <library/cpp/yt/memory/public.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TPCollection;

template <typename... T>
using TPCollections = std::tuple<TPCollection<T>...>;

class TMultiPCollection;

class TPipeline;

DECLARE_REFCOUNTED_CLASS(IExecutor);

class TGroupByKey;

struct TMultiRow;

template <typename T>
class TInput;

template <typename T>
class TInputPtr;

using TMultiInput = TInput<TMultiRow>;
using TMultiInputPtr = NYT::TIntrusivePtr<TMultiInput>;

template <typename T>
class TOutput;

using TMultiOutput = TOutput<TMultiRow>;

template <typename I, typename A, typename O>
class ICombineFn;

template <typename I, typename A, typename O>
using ICombineFnPtr = ICombineFn<I, A, O>;

template <typename T>
class TWriteApplicator;

template <typename T>
class TReadApplicator;

template <typename I, typename O>
class TParDoApplicator;

template <typename T>
class TTypeTag;

///
/// @brief Set name for given Transform
extern TTypeTag<TString> TransformNameTag;

template <typename I, typename... O>
class TApplicator;

template <typename T>
class TRenamedApplicator;

class TDynamicTypeTag;

class TCoGbkResult;

template <typename T>
class TCoder;

struct TNonCodableTag;

template <typename>
struct TIsManuallyNonCodable;

DECLARE_REFCOUNTED_CLASS(IExecutionContext);

template <typename K, typename V>
class TKV;

template <typename, typename>
class TPState;

template <typename, typename>
class TStateStore;

class TNullWriteApplicator;

class TFnAttributes;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren


namespace testing {

template <typename T>
::std::string PrintToString(const T& value);

}
