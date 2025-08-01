#pragma once

#include "public.h"

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/chaos_client/coordinator_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/transaction_service_proxy.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/client/hive/public.h>

namespace NYT::NApi::NNative {

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(IRegisterTransactionActionsRequestFactory)

//! A narrow set of functionality required by cell commit sessions.
//! The main purpose of this class is to isolate commit session code from the native client.
class IRegisterTransactionActionsRequestFactory
    : public TRefCounted
{
public:
    virtual NTabletClient::TTabletServiceProxy::TReqRegisterTransactionActionsPtr
    CreateRegisterTransactionActionsTabletCellRequest(NHiveClient::TCellId cellId) = 0;

    virtual NTransactionClient::TTransactionServiceProxy::TReqRegisterTransactionActionsPtr
    CreateRegisterTransactionActionsMasterCellRequest(NHiveClient::TCellId cellId) = 0;

    virtual NChaosClient::TCoordinatorServiceProxy::TReqRegisterTransactionActionsPtr
    CreateRegisterTransactionActionsChaosCellRequest(NHiveClient::TCellId cellId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRegisterTransactionActionsRequestFactory)

IRegisterTransactionActionsRequestFactoryPtr CreateRegisterTransactionActionsRequestFactory(
    IClientPtr client,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
