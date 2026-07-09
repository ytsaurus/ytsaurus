#pragma once

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/json/writer/json_value.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TJsonSourceComputationParameters
    : public TSwiftOrderedSourceComputation::TParameters
{
    std::string DataColumn;

    REGISTER_YSON_STRUCT(TJsonSourceComputationParameters);

    static void Register(TRegistrar registrar);
};

struct TJsonSourceComputationDynamicParameters
    : public TSwiftOrderedSourceComputation::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TJsonSourceComputationDynamicParameters);

    static void Register(TRegistrar registrar);
};

class TJsonSwiftSourceComputation
    : public TSwiftOrderedSourceComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TJsonSourceComputationParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TJsonSourceComputationDynamicParameters);

    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TInputMessageConstPtr& inputMessage, IOutputCollectorPtr output) final;

private:
    virtual void DoProcessJson(const TInputMessageConstPtr& inputMessage, NJson::TJsonValue&& inputJson, IOutputCollectorPtr output);
    virtual void DoProcessJson(NJson::TJsonValue&& inputJson, IOutputCollectorPtr output);

    virtual void DoProcessUnparsed(const TInputMessageConstPtr& message, TError error, IOutputCollectorPtr output);
};

} // namespace NYT::NFlow
