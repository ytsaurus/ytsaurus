#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/small_flat_map.h>

namespace NYT::NTableServer {

///////////////////////////////////////////////////////////////////////////////

class TMasterTableSchema
    : public NObjectServer::TNonversionedObjectBase
{
public:
    using TTableSchemaToObjectMap = THashMap<NTableClient::TTableSchema, TMasterTableSchema*>;
    using TTableSchemaToObjectMapIterator = TTableSchemaToObjectMap::iterator;
    using TAccountToMasterMemoryUsage = TSmallFlatMap<NSecurityServer::TAccount*, i64, 2>;
    using TAccountToRefCounterMap = TSmallFlatMap<NSecurityServer::TAccount*, i64, 2>;

    DEFINE_BYVAL_RO_PROPERTY(TTableSchemaToObjectMapIterator, TableSchemaToObjectMapIterator);
    // These are transient and are used for master memory accounting only.
    DEFINE_BYREF_RW_PROPERTY(TAccountToMasterMemoryUsage, ChargedMasterMemoryUsage);
    DEFINE_BYREF_RW_PROPERTY(TAccountToRefCounterMap, ReferencingAccounts);

    TMasterTableSchema(TMasterTableSchemaId id, TTableSchemaToObjectMapIterator it);
    // For persistence.
    explicit TMasterTableSchema(TMasterTableSchemaId id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const NTableClient::TTableSchema& AsTableSchema() const;
    const TFuture<NYson::TYsonString>& AsYsonAsync(const NObjectServer::TObjectManagerPtr& objectManager);
    // Whenever possible, prefer the above.
    NYson::TYsonString AsYsonSync(const NObjectServer::TObjectManagerPtr& objectManager);

    //! Increases the number of times this schema is referenced by #account.
    //! Returns true iff this schema has just become referenced by it for the
    //! first time (i.e. iff refcounter has become equal to one).
    [[nodiscard]] bool RefBy(NSecurityServer::TAccount* account);
    //! Decreases the number of times this schema is referenced by #account.
    //! Returns true iff this schema is no longer referenced by it (i.e. iff
    //! refcounter has come down to zero).
    [[nodiscard]] bool UnrefBy(NSecurityServer::TAccount* account);

    i64 GetMasterMemoryUsage(NSecurityServer::TAccount* account) const;
    i64 GetChargedMasterMemoryUsage(NSecurityServer::TAccount* account) const;
    void SetChargedMasterMemoryUsage(NSecurityServer::TAccount* account, i64 usage);

private:
    using TBase = NObjectServer::TNonversionedObjectBase;

    TFuture<NYson::TYsonString> MemoizedYson_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
