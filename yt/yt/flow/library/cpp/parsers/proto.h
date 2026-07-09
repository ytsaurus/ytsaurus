#pragma once

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

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

template <class TProto>
class TProtoSwiftSourceComputation
    : public TSwiftOrderedSourceComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TProtoSourceComputationParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicProtoSourceComputationParameters);

    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TInputMessageConstPtr& inputMessage, IOutputCollectorPtr output) final;

private:
    virtual void DoProcessProto(const TInputMessageConstPtr& inputMessage, TProto&& inputProto, IOutputCollectorPtr output);
    virtual void DoProcessProto(TProto&& inputProto, IOutputCollectorPtr output);

    virtual void DoProcessUnparsed(const TInputMessageConstPtr& inputMessage, TError error, IOutputCollectorPtr output);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define PROTO_INL_H_
#include "proto-inl.h"
#undef PROTO_INL_H_
