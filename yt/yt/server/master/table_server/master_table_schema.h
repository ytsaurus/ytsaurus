#pragma once

#include "public.h"

#include "compact_table_schema.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/yt/compact_containers/compact_flat_map.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TMasterTableSchema
    : public NObjectServer::TObject
{
public:
    using TNativeTableSchemaToObjectMap = THashMap<
        TCompactTableSchemaPtr,
        TMasterTableSchema*,
        TCompactTableSchemaHash,
        TCompactTableSchemaEquals
    >;
    using TNativeTableSchemaToObjectMapIterator = TNativeTableSchemaToObjectMap::iterator;

    using TCellTagToExportRefcount = THashMap<NObjectClient::TCellTag, int>;

    // TODO(h0pless): Change this to TCompactFlatMap.
    using TAccountToRefCounterMap = THashMap<NSecurityServer::TAccountPtr, i64>;
    using TAccountToMasterMemoryUsage = TCompactFlatMap<NSecurityServer::TAccount*, i64, 2>;

    DEFINE_BYREF_RO_PROPERTY(TCellTagToExportRefcount, CellTagToExportCount);

    DEFINE_BYREF_RO_PROPERTY(TAccountToRefCounterMap, ReferencingAccounts);

    // This field is transient and used for master memory accounting only.
    DEFINE_BYREF_RO_PROPERTY(TAccountToMasterMemoryUsage, ChargedMasterMemoryUsage);

    using TObject::TObject;
    //! Constructs a native master table schema object.
    TMasterTableSchema(TMasterTableSchemaId id, TNativeTableSchemaToObjectMapIterator it);
    //! Constructs an imported master table schema object.
    TMasterTableSchema(TMasterTableSchemaId id, TCompactTableSchemaPtr);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const TCompactTableSchemaPtr& AsCompactTableSchema(bool crashOnZombie = true) const;
    // Triggers deserialization of underlying `CompactTableSchema_`, so if possible, prefer the above.
    const NTableClient::TTableSchema& AsTableSchema(bool crashOnZombie = true) const;
    const TFuture<NYson::TYsonString>& AsYsonAsync() const;
    // Whenever possible, prefer the above.
    NYson::TYsonString AsYsonSync() const;

    //! Increases the number of times this schema is referenced by #account by #delta.
    //! Returns true iff this schema has just become referenced by it for the
    //! first time (i.e. iff refcounter has changed from zero).
    [[nodiscard]] bool RefBy(NSecurityServer::TAccount* account, int delta = 1);
    //! Decreases the number of times this schema is referenced by #account by #delta.
    //! Returns true iff this schema is no longer referenced by it (i.e. iff
    //! refcounter has come down to zero).
    [[nodiscard]] bool UnrefBy(NSecurityServer::TAccount* account, int delta = 1);

    bool IsExported(NObjectClient::TCellTag cellTag) const;

    void AlertIfNonEmptyExportCount();

    i64 GetMasterMemoryUsage(NSecurityServer::TAccount* account) const;
    i64 GetChargedMasterMemoryUsage(NSecurityServer::TAccount* account) const;
    void SetChargedMasterMemoryUsage(NSecurityServer::TAccount* account, i64 usage);

private:
    friend class TTableManager;

    using TBase = NObjectServer::TObject;

    // NB: only used for native (non-foreign) schema objects.
    TNativeTableSchemaToObjectMapIterator NativeTableSchemaToObjectMapIterator_;

    TCompactTableSchemaPtr CompactTableSchema_;

    mutable TFuture<NYson::TYsonString> MemoizedYson_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MemoizedYsonLock_);

    TNativeTableSchemaToObjectMapIterator GetNativeTableSchemaToObjectMapIterator() const;
    void SetNativeTableSchemaToObjectMapIterator(TNativeTableSchemaToObjectMapIterator it);
    void ResetNativeTableSchemaToObjectMapIterator();

    //! Increments export ref counter.
    void ExportRef(NObjectClient::TCellTag cellTag);

    //! Decrements export ref counter.
    void UnexportRef(NObjectClient::TCellTag cellTag, int decreaseBy = 1);
};

DEFINE_MASTER_OBJECT_TYPE(TMasterTableSchema)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
