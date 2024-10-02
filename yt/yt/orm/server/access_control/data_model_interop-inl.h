#ifndef DATA_MODEL_INTEROP_INL_H_
#error "Direct inclusion of this file is not allowed, include data_model_interop.h"
// For the sake of sane code completion.
#include "data_model_interop.h"
#endif

#include "private.h"

#include "cast.h"
#include "object_cluster.h"
#include "subject_cluster.h"

#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/orm/server/objects/db_schema.h>
#include <yt/yt/orm/server/objects/key_util.h>
#include <yt/yt/orm/server/objects/type_handler.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <
    class TUsersTable,
    class TGroupsTable,
    class TProtoUserSpec,
    class TProtoGroupSpec,
    class TProtoAccessControlEntry>
class TDataModelInterop
    : public IDataModelInterop
{
public:
    TDataModelInterop(
        const TUsersTable* usersTable,
        const TGroupsTable* groupsTable)
        : UsersTable_(usersTable)
        , GroupsTable_(groupsTable)
    { }

    TString GetUserQueryString(const NMaster::TYTConnectorPtr& ytConnector) override
    {
        return Format(
            "[%v], [%v] from [%v] where is_null([%v])",
            UsersTable_->Fields.MetaId.Name,
            UsersTable_->Fields.Spec.Name,
            ytConnector->GetTablePath(UsersTable_),
            UsersTable_->Fields.MetaRemovalTime.Name);
    }

    std::unique_ptr<TSnapshotUser> ParseUser(NTableClient::TUnversionedRow row) override
    {
        TObjectId id;
        TProtoUserSpec spec;
        NTableClient::FromUnversionedRow(
            row,
            &id,
            &spec);
        return std::make_unique<TSnapshotUser>(
            std::move(id),
            spec.banned(),
            YT_PROTO_OPTIONAL(spec, request_weight_rate_limit),
            YT_PROTO_OPTIONAL(spec, request_queue_size_limit),
            YT_PROTO_OPTIONAL(spec, execution_pool));
    }

    TString GetGroupQueryString(const NMaster::TYTConnectorPtr& ytConnector) override
    {
        return Format(
            "[%v], [%v] from [%v] where is_null([%v])",
            GroupsTable_->Fields.MetaId.Name,
            GroupsTable_->Fields.Spec.Name,
            ytConnector->GetTablePath(GroupsTable_),
            GroupsTable_->Fields.MetaRemovalTime.Name);
    }

    std::unique_ptr<TSnapshotGroup> ParseGroup(NTableClient::TUnversionedRow row) override
    {
        TObjectId id;
        TProtoGroupSpec spec;
        NTableClient::FromUnversionedRow(
            row,
            &id,
            &spec);
        std::vector<TObjectId> members;
        NYT::FromProto(&members, spec.members());
        return std::make_unique<TSnapshotGroup>(
            std::move(id),
            std::move(members));
    }

    std::vector<const NObjects::TDBField*> GetObjectQueryFields(
        NObjects::IObjectTypeHandler* typeHandler) override
    {
        using NObjects::ObjectsTable;

        std::vector<const NObjects::TDBField*> fields = typeHandler->GetKeyFields();
        fields.push_back(&ObjectsTable.Fields.MetaAcl);
        fields.push_back(&ObjectsTable.Fields.MetaInheritAcl);
        fields.push_back(&ObjectsTable.Fields.Labels);
        if (typeHandler->GetAccessControlParentType() != TObjectTypeValues::Null) {
            auto parentKeyFields = typeHandler->GetAccessControlParentKeyFields();
            fields.insert(fields.end(), parentKeyFields.begin(), parentKeyFields.end());
        }
        return fields;
    }

    std::unique_ptr<TSnapshotObject> ParseObject(
        NObjects::IObjectTypeHandler* typeHandler,
        NTableClient::TUnversionedRow row) override
    {
        TObjectKey key;
        std::vector<TProtoAccessControlEntry> protoAcl;
        bool inheritAcl;
        NYson::TYsonString labels;
        TObjectKey accessControlParentKey;

        if (typeHandler->GetAccessControlParentType() != TObjectTypeValues::Null) {
            NObjects::FromUnversionedRow(
                row,
                &key,
                typeHandler->GetKeyFields().size(),
                &protoAcl,
                &inheritAcl,
                &labels,
                &accessControlParentKey,
                typeHandler->GetAccessControlParentKeyFields().size());
        } else {
            NObjects::FromUnversionedRow(
                row,
                &key,
                typeHandler->GetKeyFields().size(),
                &protoAcl,
                &inheritAcl,
                &labels);
        }

        TAccessControlList acl;
        acl.resize(protoAcl.size());
        for (size_t i = 0; i < protoAcl.size(); ++i) {
            FromProto(&acl[i], protoAcl[i]);
        }

        return std::make_unique<TSnapshotObject>(
            typeHandler->GetType(),
            std::move(key),
            std::move(accessControlParentKey),
            std::move(acl),
            inheritAcl,
            std::move(labels));
    }

private:
    const TUsersTable* const UsersTable_;
    const TGroupsTable* const GroupsTable_;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <
    class TUsersTable,
    class TGroupsTable,
    class TProtoUserSpec,
    class TProtoGroupSpec,
    class TProtoAccessControlEntry>
IDataModelInteropPtr CreateDataModelInterop(
    const TUsersTable* usersTable,
    const TGroupsTable* groupsTable)
{
    return New<NDetail::TDataModelInterop<
        TUsersTable,
        TGroupsTable,
        TProtoUserSpec,
        TProtoGroupSpec,
        TProtoAccessControlEntry>>(usersTable, groupsTable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
