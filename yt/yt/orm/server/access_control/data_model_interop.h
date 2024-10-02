#pragma once

#include "public.h"

#include <yt/yt/orm/server/master/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

struct IDataModelInterop
    : public TRefCounted
{
    virtual TString GetUserQueryString(
        const NMaster::TYTConnectorPtr& ytConnector) = 0;
    virtual std::unique_ptr<TSnapshotUser> ParseUser(
        NTableClient::TUnversionedRow row) = 0;

    virtual TString GetGroupQueryString(
        const NMaster::TYTConnectorPtr& ytConnector) = 0;
    virtual std::unique_ptr<TSnapshotGroup> ParseGroup(
        NTableClient::TUnversionedRow row) = 0;

    virtual std::vector<const NObjects::TDBField*> GetObjectQueryFields(
        NObjects::IObjectTypeHandler* typeHandler) = 0;
    virtual std::unique_ptr<TSnapshotObject> ParseObject(
        NObjects::IObjectTypeHandler* typeHandler,
        NTableClient::TUnversionedRow row) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDataModelInterop)

////////////////////////////////////////////////////////////////////////////////

template <
    class TUsersTable,
    class TGroupsTable,
    class TProtoUserSpec,
    class TProtoGroupSpec,
    class TProtoAccessControlEntry>
IDataModelInteropPtr CreateDataModelInterop(
    const TUsersTable* usersTable,
    const TGroupsTable* groupsTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl

#define DATA_MODEL_INTEROP_INL_H_
#include "data_model_interop-inl.h"
#undef DATA_MODEL_INTEROP_INL_H_
