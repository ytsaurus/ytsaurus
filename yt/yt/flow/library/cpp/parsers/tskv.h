#pragma once

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TTskvSourceComputationParameters
    : public TSwiftOrderedSourceComputation::TParameters
{
    std::string DataColumn;
    std::string TskvMarker;

    REGISTER_YSON_STRUCT(TTskvSourceComputationParameters);

    static void Register(TRegistrar registrar);
};

struct TTskvSourceComputationDynamicParameters
    : public TSwiftOrderedSourceComputation::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TTskvSourceComputationDynamicParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TTskvSwiftSourceComputation
    : public TSwiftOrderedSourceComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TTskvSourceComputationParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TTskvSourceComputationDynamicParameters);

    TTskvSwiftSourceComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext,
        THashSet<TStringBuf> requiredFields = {});

    void DoProcessMessage(const TInputMessageConstPtr& inputMessage, IOutputCollectorPtr output) final;

private:
    virtual void DoProcessTskv(
        const TInputMessageConstPtr& inputMessage,
        THashMap<TStringBuf, TStringBuf>&& inputTskv,
        IOutputCollectorPtr output);
    virtual void DoProcessTskv(
        THashMap<TStringBuf, TStringBuf>&& inputTskv,
        IOutputCollectorPtr output);

    virtual void DoProcessUnparsed(const TInputMessageConstPtr& inputMessage, TError error, IOutputCollectorPtr output);

private:
    const THashSet<TStringBuf> RequiredFields_;
};

} // namespace NYT::NFlow
