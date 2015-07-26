#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <core/actions/signal.h>

#include <core/rpc/public.h>

#include <ytlib/election/public.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMulticellManager
    : public TRefCounted
{
public:
    TMulticellManager(
        TBootstrap* bootstrap,
        TCellMasterConfigPtr config);
    ~TMulticellManager();

    void PostToPrimaryMaster(
        const ::google::protobuf::MessageLite& requestMessage,
        bool reliable = true);

    void PostToSecondaryMaster(
        NRpc::IClientRequestPtr request,
        NObjectClient::TCellTag cellTag,
        bool reliable = true);
    void PostToSecondaryMaster(
        const NObjectClient::TObjectId& objectId,
        NRpc::IServiceContextPtr context,
        NObjectClient::TCellTag cellTag,
        bool reliable = true);
    void PostToSecondaryMaster(
        const ::google::protobuf::MessageLite& requestMessage,
        NObjectClient::TCellTag cellTag,
        bool reliable = true);
    void PostToSecondaryMaster(
        TSharedRefArray requestMessage,
        NObjectClient::TCellTag cellTag,
        bool reliable = true);

    void PostToSecondaryMasters(
        NRpc::IClientRequestPtr request,
        bool reliable = true);
    void PostToSecondaryMasters(
        const NObjectClient::TObjectId& objectId,
        NRpc::IServiceContextPtr context,
        bool reliable = true);
    void PostToSecondaryMasters(
        const ::google::protobuf::MessageLite& requestMessage,
        bool reliable = true);
    void PostToSecondaryMasters(
        TSharedRefArray requestMessage,
        bool reliable = true);

    DECLARE_SIGNAL(void(NObjectClient::TCellTag), SecondaryMasterRegistered);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TMulticellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
