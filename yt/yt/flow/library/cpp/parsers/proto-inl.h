#pragma once

#ifndef PROTO_INL_H_
    #error "Direct inclusion of this file is not allowed, include proto.h"
    // For the sake of sane code completion.
    #include "proto.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TProto, class TOnProto, class TOnUnparsed>
void ParseProtoColumnPropagatingHookErrors(
    const TMessage& message,
    TStringBuf dataColumn,
    const TOnProto& onProto,
    const TOnUnparsed& onUnparsed)
{
    std::optional<TStringBuf> rawData;
    try {
        rawData = GetColumnValue<std::optional<TStringBuf>>(message, dataColumn);
    } catch (const std::exception& ex) {
        onUnparsed(TError(ex).With("data_column", dataColumn));
        return;
    }
    if (!rawData) {
        onUnparsed(TError("empty data"));
        return;
    }

    TProto proto;
    if (!proto.ParseFromArray(rawData->data(), rawData->size())) {
        auto error = TError("Failed to parse protobuf message %v", proto.GetTypeName())
            .With("data_size", rawData->size());
        if (!proto.IsInitialized()) {
            error <<= TErrorAttribute("initialization_error", proto.InitializationErrorString());
        }
        onUnparsed(std::move(error));
        return;
    }

    onProto(std::move(proto));
}

template <class TProto, class TOnProto, class TOnUnparsed>
void ParseProtoColumnRoutingHookErrors(
    const TMessage& message,
    TStringBuf dataColumn,
    const TOnProto& onProto,
    const TOnUnparsed& onUnparsed)
{
    ParseProtoColumnPropagatingHookErrors<TProto>(
        message,
        dataColumn,
        [&] (TProto&& proto) {
            try {
                onProto(std::move(proto));
            } catch (const std::exception& ex) {
                onUnparsed(TError(ex));
            }
        },
        onUnparsed);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TBase, class TProto, class TProtoParameters, class TDynamicProtoParameters, bool PropagateHookErrors>
void TProtoParsingComputationBase<TBase, TProto, TProtoParameters, TDynamicProtoParameters, PropagateHookErrors>::DoProcessMessage(
    const TInputMessageConstPtr& inputMessage,
    IOutputCollectorPtr output)
{
    auto onProto = [&] (TProto&& inputProto) {
        DoProcessProto(inputMessage, std::move(inputProto), output);
    };
    auto onUnparsed = [&] (TError error) {
        DoProcessUnparsed(inputMessage, std::move(error), output);
    };
    if constexpr (PropagateHookErrors) {
        NDetail::ParseProtoColumnPropagatingHookErrors<TProto>(*inputMessage, GetParameters()->DataColumn, onProto, onUnparsed);
    } else {
        NDetail::ParseProtoColumnRoutingHookErrors<TProto>(*inputMessage, GetParameters()->DataColumn, onProto, onUnparsed);
    }
}

template <class TBase, class TProto, class TProtoParameters, class TDynamicProtoParameters, bool PropagateHookErrors>
void TProtoParsingComputationBase<TBase, TProto, TProtoParameters, TDynamicProtoParameters, PropagateHookErrors>::DoProcessProto(const TInputMessageConstPtr& /*inputMessage*/, TProto&& inputProto, IOutputCollectorPtr output)
{
    DoProcessProto(std::forward<TProto>(inputProto), std::move(output));
}

template <class TBase, class TProto, class TProtoParameters, class TDynamicProtoParameters, bool PropagateHookErrors>
void TProtoParsingComputationBase<TBase, TProto, TProtoParameters, TDynamicProtoParameters, PropagateHookErrors>::DoProcessProto(TProto&&, IOutputCollectorPtr /*output*/)
{
    THROW_ERROR_EXCEPTION("One of the overloads for DoProcessProto must be implemented");
}

template <class TBase, class TProto, class TProtoParameters, class TDynamicProtoParameters, bool PropagateHookErrors>
void TProtoParsingComputationBase<TBase, TProto, TProtoParameters, TDynamicProtoParameters, PropagateHookErrors>::DoProcessUnparsed(const TInputMessageConstPtr& /*inputMessage*/, TError error, IOutputCollectorPtr /*output*/)
{
    THROW_ERROR error;
}

////////////////////////////////////////////////////////////////////////////////

template <class TProto, class TParameters, bool PropagateHookErrors>
void TProtoParsingProcessFunctionBase<TProto, TParameters, PropagateHookErrors>::Init(const IRuntimeInitContextPtr& initContext)
{
    Parameters_ = initContext->GetParameters<TParameters>();
    DoInit(initContext);
}

template <class TProto, class TParameters, bool PropagateHookErrors>
void TProtoParsingProcessFunctionBase<TProto, TParameters, PropagateHookErrors>::DoInit(const IRuntimeInitContextPtr& /*initContext*/)
{ }

template <class TProto, class TParameters, bool PropagateHookErrors>
const TIntrusivePtr<TParameters>& TProtoParsingProcessFunctionBase<TProto, TParameters, PropagateHookErrors>::GetParameters() const
{
    return Parameters_;
}

template <class TProto, class TParameters, bool PropagateHookErrors>
void TProtoParsingProcessFunctionBase<TProto, TParameters, PropagateHookErrors>::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto onProto = [&] (TProto&& proto) {
        ProcessProto(message, std::move(proto), output, context);
    };
    auto onUnparsed = [&] (TError error) {
        ProcessUnparsed(message, std::move(error), output, context);
    };
    if constexpr (PropagateHookErrors) {
        NDetail::ParseProtoColumnPropagatingHookErrors<TProto>(*message, Parameters_->DataColumn, onProto, onUnparsed);
    } else {
        NDetail::ParseProtoColumnRoutingHookErrors<TProto>(*message, Parameters_->DataColumn, onProto, onUnparsed);
    }
}

template <class TProto, class TParameters, bool PropagateHookErrors>
void TProtoParsingProcessFunctionBase<TProto, TParameters, PropagateHookErrors>::ProcessUnparsed(
    const TInputMessageConstPtr& /*message*/,
    TError error,
    const IOutputCollectorPtr& /*output*/,
    const IRuntimeContextPtr& /*context*/)
{
    THROW_ERROR error;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
