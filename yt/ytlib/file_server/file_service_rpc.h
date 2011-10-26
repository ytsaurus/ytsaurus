#pragma once

#include "common.h"

#include "common.h"
#include "file_service_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TFileServiceProxy> TPtr;

    RPC_DECLARE_PROXY(FileService,
        ((NoSuchTransaction)(1))
        ((NoSuchNode)(2))
        ((NoSuchChunk)(3))
        ((InvalidNodeType)(4))
    );

    TFileServiceProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, SetFileChunk);
    RPC_PROXY_METHOD(NProto, GetFileChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

