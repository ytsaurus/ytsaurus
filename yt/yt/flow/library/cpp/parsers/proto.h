#pragma once

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TProtoSourceComputationParameters
    : public NYT::NFlow::TSwiftOrderedSourceComputation::TParameters
{
    std::string DataColumn;

    REGISTER_YSON_STRUCT(TProtoSourceComputationParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TDynamicProtoSourceComputationParameters
    : public NYT::NFlow::TSwiftOrderedSourceComputation::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TDynamicProtoSourceComputationParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicProtoSourceComputationParameters);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TProto, class TOnProto, class TOnUnparsed>
void ParseProtoColumnPropagatingHookErrors(
    const TMessage& message,
    TStringBuf dataColumn,
    const TOnProto& onProto,
    const TOnUnparsed& onUnparsed);

template <class TProto, class TOnProto, class TOnUnparsed>
void ParseProtoColumnRoutingHookErrors(
    const TMessage& message,
    TStringBuf dataColumn,
    const TOnProto& onProto,
    const TOnUnparsed& onUnparsed);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <
    class TBase,
    class TProto,
    class TProtoParameters = TProtoSourceComputationParameters,
    class TDynamicProtoParameters = TDynamicProtoSourceComputationParameters,
    bool PropagateHookErrors = false>
class TProtoParsingComputationBase
    : public TBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TProtoParameters, TBase);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicProtoParameters, TBase);

    using TBase::TBase;

    void DoProcessMessage(const TInputMessageConstPtr& inputMessage, IOutputCollectorPtr output) final;

private:
    virtual void DoProcessProto(const TInputMessageConstPtr& inputMessage, TProto&& inputProto, IOutputCollectorPtr output);
    virtual void DoProcessProto(TProto&& inputProto, IOutputCollectorPtr output);

    virtual void DoProcessUnparsed(const TInputMessageConstPtr& inputMessage, TError error, IOutputCollectorPtr output);
};

////////////////////////////////////////////////////////////////////////////////

template <class TProto>
class TProtoSwiftSourceComputation
    : public TProtoParsingComputationBase<TSwiftOrderedSourceComputation, TProto>
{
public:
    using TProtoParsingComputationBase<TSwiftOrderedSourceComputation, TProto>::TProtoParsingComputationBase;
};

////////////////////////////////////////////////////////////////////////////////

struct TProtoTransformSourceComputationParameters
    : public TTransformOrderedSourceComputation::TParameters
{
    std::string DataColumn;

    REGISTER_YSON_STRUCT(TProtoTransformSourceComputationParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TDynamicProtoTransformSourceComputationParameters
    : public TTransformOrderedSourceComputation::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TDynamicProtoTransformSourceComputationParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicProtoTransformSourceComputationParameters);

////////////////////////////////////////////////////////////////////////////////

template <class TProto>
class TProtoTransformOrderedSourceComputation
    : public TProtoParsingComputationBase<
          TTransformOrderedSourceComputation,
          TProto,
          TProtoTransformSourceComputationParameters,
          TDynamicProtoTransformSourceComputationParameters,
          /*PropagateHookErrors*/ true>
{
public:
    using TProtoParsingComputationBase<
        TTransformOrderedSourceComputation,
        TProto,
        TProtoTransformSourceComputationParameters,
        TDynamicProtoTransformSourceComputationParameters,
        true>::TProtoParsingComputationBase;
};

////////////////////////////////////////////////////////////////////////////////

//! Static parameters for #TProtoParsingProcessFunctionBase: the process-function analog of
//! #TProtoSourceComputationParameters.
struct TProtoParsingProcessFunctionParameters
    : public NYTree::TYsonStruct
{
    std::string DataColumn;

    REGISTER_YSON_STRUCT(TProtoParsingProcessFunctionParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Process-function analog of #TProtoParsingComputationBase: parses the message's #TProto out of
//! the configured column (``data_column`` in the static ``processing_function_parameters``,
//! defaulting to ``"data"``) before dispatching to #ProcessProto. A column read/parse failure is
//! routed to #ProcessUnparsed instead (default: rethrow, matching #TProtoParsingComputationBase).
//!
//! |PropagateHookErrors| selects how an exception from #ProcessProto itself (as opposed to a
//! parse failure) is handled: |false| routes it to #ProcessUnparsed like a parse failure; |true|
//! lets it escape #ProcessMessage and fail the epoch. Pick |true| when #ProcessProto can fail for
//! reasons other than malformed input, so such failures aren't misreported as bad input.
//!
//! |TParameters| must derive from #TProtoParsingProcessFunctionParameters (directly, or through a
//! function-specific static parameters struct, mirroring how a computation extends
//! #TProtoSourceComputationParameters) and must be the exact type the owning function registers as
//! its static parameters via ``YT_FLOW_DEFINE_PROCESS_FUNCTION(Function, TParameters, ...)``.
//!
//! Every input stream must carry the configured column: a message from a stream without it is
//! routed to #ProcessUnparsed as a read failure, which by default wedges the epoch forever. In
//! practice this means a single input stream.
template <
    class TProto,
    class TParameters = TProtoParsingProcessFunctionParameters,
    bool PropagateHookErrors = false>
class TProtoParsingProcessFunctionBase
    : public IProcessFunction
{
public:
    static_assert(std::is_base_of_v<TProtoParsingProcessFunctionParameters, TParameters>);

    //! Sole setter of the parameters #ProcessMessage reads; hook into initialization via #DoInit.
    void Init(const IRuntimeInitContextPtr& initContext) final;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) final;

protected:
    //! Initialization hook, called once by #Init after parameters are fetched. Default: no-op.
    virtual void DoInit(const IRuntimeInitContextPtr& initContext);

    const TIntrusivePtr<TParameters>& GetParameters() const;

    virtual void ProcessProto(
        const TInputMessageConstPtr& message,
        TProto&& proto,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) = 0;

    //! Default: rethrows, matching #TProtoParsingComputationBase's DoProcessUnparsed. Override to
    //! skip/log instead; an override that only wants to special-case some errors can fall back to
    //! this default via TProtoParsingProcessFunctionBase::ProcessUnparsed(...). With
    //! |PropagateHookErrors| = |false|, a skipping override also swallows #ProcessProto's own
    //! failures.
    virtual void ProcessUnparsed(
        const TInputMessageConstPtr& message,
        TError error,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context);

private:
    TIntrusivePtr<TParameters> Parameters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define PROTO_INL_H_
#include "proto-inl.h"
#undef PROTO_INL_H_
