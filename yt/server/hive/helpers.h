#pragma once

#include "public.h"

#include <yt/ytlib/hive/hive_service.pb.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

NHiveClient::NProto::TEncapsulatedMessage SerializeMessage(
    const ::google::protobuf::MessageLite& message);

////////////////////////////////////////////////////////////////////////////////

template <class TCallback>
struct TTransactionActionHandlerDescriptor
{
    TString Type;
    TCallback Handler;
};

template <class TTransaction*, class TProto, class... TArgs>
TTransactionActionHandlerDescriptor<TCallback<void(TTransaction*, const TString&, TArgs...)>>
MakeTransactionActionHandlerDescriptor(
    TCallback<void(TTransaction*, TProto*, TArgs...)> handler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
