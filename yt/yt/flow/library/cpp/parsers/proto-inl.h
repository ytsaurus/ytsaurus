#pragma once

#ifndef PROTO_INL_H_
    #error "Direct inclusion of this file is not allowed, include proto.h"
    // For the sake of sane code completion.
    #include "proto.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TProto>
void TProtoSwiftSourceComputation<TProto>::DoProcessMessage(const TInputMessageConstPtr& inputMessage, IOutputCollectorPtr output)
{
    auto rawData = GetColumnValue<std::optional<TStringBuf>>(inputMessage, GetParameters()->DataColumn);
    if (!rawData) {
        DoProcessUnparsed(inputMessage, TError("empty data"), output);
        return;
    }

    try {
        TProto proto;
        proto.ParseFromStringOrThrow(*rawData);
        DoProcessProto(inputMessage, std::move(proto), output);
    } catch (const std::exception& ex) {
        DoProcessUnparsed(inputMessage, TError(ex), output);
    }
}

template <class TProto>
void TProtoSwiftSourceComputation<TProto>::DoProcessProto(const TInputMessageConstPtr& /*inputMessage*/, TProto&& inputProto, IOutputCollectorPtr output)
{
    DoProcessProto(std::forward<TProto>(inputProto), std::move(output));
}

template <class TProto>
void TProtoSwiftSourceComputation<TProto>::DoProcessProto(TProto&&, IOutputCollectorPtr /*output*/)
{
    THROW_ERROR_EXCEPTION("One of the overloads for DoProcessProto must be implemented");
}

template <class TProto>
void TProtoSwiftSourceComputation<TProto>::DoProcessUnparsed(const TInputMessageConstPtr& /*inputMessage*/, TError error, IOutputCollectorPtr /*output*/)
{
    THROW_ERROR error;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
