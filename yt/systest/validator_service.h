#pragma once

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/systest/proto/validator.pb.h>

namespace NYT::NTest {

class TValidatorProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TValidatorProxy, Validator);

    DEFINE_RPC_PROXY_METHOD(NProto, MapInterval);
    DEFINE_RPC_PROXY_METHOD(NProto, ReduceInterval);
    DEFINE_RPC_PROXY_METHOD(NProto, SortInterval);
    DEFINE_RPC_PROXY_METHOD(NProto, MergeSortedAndCompare);
    DEFINE_RPC_PROXY_METHOD(NProto, CompareInterval);
};

///////////////////////////////////////////////////////////////////////////////

void RunValidatorService(IClientPtr client, int port);

}  // namespace NYT::NTest
