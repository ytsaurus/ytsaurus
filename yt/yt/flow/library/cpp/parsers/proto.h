#pragma once

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_ordered_source_computation.h>

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

} // namespace NYT::NFlow

#define PROTO_INL_H_
#include "proto-inl.h"
#undef PROTO_INL_H_
