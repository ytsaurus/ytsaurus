// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include "objects.h"
#include "object_detail.h"

#include <yt/yt/orm/example/plugins/server/library/custom_base_type_handler.h>

#include <yt/yt/orm/server/access_control/access_control_manager.h>
#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/objects/public.h>
#include <yt/yt/orm/server/objects/type_handler.h>
#include <yt/yt/orm/server/objects/type_handler_detail.h>

#include <memory>

namespace NYT::NOrm::NExample::NServer::NLibrary {

using NYT::NOrm::NServer::NObjects::TObjectTypeHandlerBase;

////////////////////////////////////////////////////////////////////////////////

static_assert(std::is_base_of<TObjectTypeHandlerBase, NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase>());

class TAuthorTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TAuthorTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::vector<NYT::NOrm::NServer::NAccessControl::TAccessControlEntry> GetDefaultAcl() override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TAuthor* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TAuthor* object,
        const TAuthor::TMetaEtc& metaEtcOld,
        const TAuthor::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TAuthor::TMetaEtc& metaEtcOld,
        const TAuthor::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TBookTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TBookTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;
    NYT::NOrm::NClient::NObjects::TObjectTypeValue GetParentType() const override;

    const TDBFields& GetParentKeyFields() const override;

    NYT::NOrm::NServer::NObjects::TObject* GetParent(
        const NYT::NOrm::NServer::NObjects::TObject* object,
        std::source_location location = std::source_location::current()) override;

    std::vector<NYT::NOrm::NServer::NAccessControl::TAccessControlEntry> GetDefaultAcl() override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;
    bool AreTagsEnabled() const override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const TDBFields ParentKeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TBook* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TBook* object,
        const TBook::TMetaEtc& metaEtcOld,
        const TBook::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TBook::TMetaEtc& metaEtcOld,
        const TBook::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TBufferedTimestampIdTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TBufferedTimestampIdTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TBufferedTimestampId* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TBufferedTimestampId* object,
        const TBufferedTimestampId::TMetaEtc& metaEtcOld,
        const TBufferedTimestampId::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TBufferedTimestampId::TMetaEtc& metaEtcOld,
        const TBufferedTimestampId::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TCatTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TCatTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);
    virtual ~TCatTypeHandler() = 0;

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TCat* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TCat* object,
        const TCat::TMetaEtc& metaEtcOld,
        const TCat::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TCat::TMetaEtc& metaEtcOld,
        const TCat::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TEditorTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TEditorTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);
    virtual ~TEditorTypeHandler() = 0;

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TEditor* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TEditor* object,
        const TEditor::TMetaEtc& metaEtcOld,
        const TEditor::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TEditor::TMetaEtc& metaEtcOld,
        const TEditor::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TEmployerTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TEmployerTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);
    virtual ~TEmployerTypeHandler() = 0;

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;
    bool AreTagsEnabled() const override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TEmployer* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TEmployer* object,
        const TEmployer::TMetaEtc& metaEtcOld,
        const TEmployer::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TEmployer::TMetaEtc& metaEtcOld,
        const TEmployer::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TExecutorTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TExecutorTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TExecutor* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TExecutor* object,
        const TExecutor::TMetaEtc& metaEtcOld,
        const TExecutor::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TExecutor::TMetaEtc& metaEtcOld,
        const TExecutor::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TGenreTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TGenreTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);
    virtual ~TGenreTypeHandler() = 0;

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TGenre* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TGenre* object,
        const TGenre::TMetaEtc& metaEtcOld,
        const TGenre::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TGenre::TMetaEtc& metaEtcOld,
        const TGenre::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TGroupTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TGroupTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TGroup* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TGroup* object,
        const TGroup::TMetaEtc& metaEtcOld,
        const TGroup::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TGroup::TMetaEtc& metaEtcOld,
        const TGroup::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class THitchhikerTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    THitchhikerTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const THitchhiker* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const THitchhiker* object,
        const THitchhiker::TMetaEtc& metaEtcOld,
        const THitchhiker::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const THitchhiker::TMetaEtc& metaEtcOld,
        const THitchhiker::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TIllustratorTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TIllustratorTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;
    NYT::NOrm::NClient::NObjects::TObjectTypeValue GetParentType() const override;

    const TDBFields& GetParentKeyFields() const override;

    NYT::NOrm::NServer::NObjects::TObject* GetParent(
        const NYT::NOrm::NServer::NObjects::TObject* object,
        std::source_location location = std::source_location::current()) override;


    NYT::NOrm::NClient::NObjects::TObjectTypeValue GetAccessControlParentType() const override;

    NYT::NOrm::NServer::NObjects::TObject* GetAccessControlParent(
        const NYT::NOrm::NServer::NObjects::TObject* object,
        std::source_location location = std::source_location::current()) override;

    void ScheduleAccessControlParentKeyLoad(
        const NYT::NOrm::NServer::NObjects::TObject* object) override;

    const TDBFields& GetAccessControlParentKeyFields() override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const TDBFields ParentKeyFields_;
    const TDBFields AccessControlParentKeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TIllustrator* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TIllustrator* object,
        const TIllustrator::TMetaEtc& metaEtcOld,
        const TIllustrator::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TIllustrator::TMetaEtc& metaEtcOld,
        const TIllustrator::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TIndexedIncrementIdTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TIndexedIncrementIdTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TIndexedIncrementId* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TIndexedIncrementId* object,
        const TIndexedIncrementId::TMetaEtc& metaEtcOld,
        const TIndexedIncrementId::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TIndexedIncrementId::TMetaEtc& metaEtcOld,
        const TIndexedIncrementId::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TInterceptorTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TInterceptorTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;
    NYT::NOrm::NClient::NObjects::TObjectTypeValue GetParentType() const override;

    const TDBFields& GetParentKeyFields() const override;

    NYT::NOrm::NServer::NObjects::TObject* GetParent(
        const NYT::NOrm::NServer::NObjects::TObject* object,
        std::source_location location = std::source_location::current()) override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const TDBFields ParentKeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TInterceptor* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TInterceptor* object,
        const TInterceptor::TMetaEtc& metaEtcOld,
        const TInterceptor::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TInterceptor::TMetaEtc& metaEtcOld,
        const TInterceptor::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TManualIdTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TManualIdTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);
    virtual ~TManualIdTypeHandler() = 0;

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TManualId* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TManualId* object,
        const TManualId::TMetaEtc& metaEtcOld,
        const TManualId::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TManualId::TMetaEtc& metaEtcOld,
        const TManualId::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TMotherShipTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TMotherShipTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);
    virtual ~TMotherShipTypeHandler() = 0;

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;
    NYT::NOrm::NClient::NObjects::TObjectTypeValue GetParentType() const override;

    const TDBFields& GetParentKeyFields() const override;

    NYT::NOrm::NServer::NObjects::TObject* GetParent(
        const NYT::NOrm::NServer::NObjects::TObject* object,
        std::source_location location = std::source_location::current()) override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const TDBFields ParentKeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TMotherShip* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TMotherShip* object,
        const TMotherShip::TMetaEtc& metaEtcOld,
        const TMotherShip::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TMotherShip::TMetaEtc& metaEtcOld,
        const TMotherShip::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TMultipolicyIdTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TMultipolicyIdTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TMultipolicyId* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TMultipolicyId* object,
        const TMultipolicyId::TMetaEtc& metaEtcOld,
        const TMultipolicyId::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TMultipolicyId::TMetaEtc& metaEtcOld,
        const TMultipolicyId::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TNestedColumnsTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TNestedColumnsTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TNestedColumns* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TNestedColumns* object,
        const TNestedColumns::TMetaEtc& metaEtcOld,
        const TNestedColumns::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TNestedColumns::TMetaEtc& metaEtcOld,
        const TNestedColumns::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TNexusTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TNexusTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TNexus* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TNexus* object,
        const TNexus::TMetaEtc& metaEtcOld,
        const TNexus::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TNexus::TMetaEtc& metaEtcOld,
        const TNexus::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TNirvanaDMProcessInstanceTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TNirvanaDMProcessInstanceTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TNirvanaDMProcessInstance* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TNirvanaDMProcessInstance* object,
        const TNirvanaDMProcessInstance::TMetaEtc& metaEtcOld,
        const TNirvanaDMProcessInstance::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TNirvanaDMProcessInstance::TMetaEtc& metaEtcOld,
        const TNirvanaDMProcessInstance::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TPublisherTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TPublisherTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);
    virtual ~TPublisherTypeHandler() = 0;

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TPublisher* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TPublisher* object,
        const TPublisher::TMetaEtc& metaEtcOld,
        const TPublisher::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TPublisher::TMetaEtc& metaEtcOld,
        const TPublisher::TMetaEtc& metaEtcNew);

    void DoPrepareAttributeMigrations(
        NYT::NOrm::NServer::NObjects::TObject* object,
        const TBitSet<int>& attributeMigrations,
        const TBitSet<int>& forcedAttributeMigrations) override;

    void DoFinalizeAttributeMigrations(
        NYT::NOrm::NServer::NObjects::TObject* object,
        const TBitSet<int>& attributeMigrations,
        const TBitSet<int>& forcedAttributeMigrations) override;

    virtual void DoPrepareSpecNumberOfAwardsToStatusNumberOfAwardsMigration(TPublisher* object) = 0;

    virtual void DoFinalizeSpecNumberOfAwardsToStatusNumberOfAwardsMigration(TPublisher* object) = 0;
    virtual void DoReverseSpecNumberOfAwardsToStatusNumberOfAwardsMigration(TPublisher* object) = 0;

    virtual void DoPrepareSpecFeaturedIllustratorsToStatusFeaturedIllustratorsMigration(TPublisher* object) = 0;

    virtual void DoFinalizeSpecFeaturedIllustratorsToStatusFeaturedIllustratorsMigration(TPublisher* object) = 0;
    virtual void DoReverseSpecFeaturedIllustratorsToStatusFeaturedIllustratorsMigration(TPublisher* object) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TRandomIdTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TRandomIdTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TRandomId* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TRandomId* object,
        const TRandomId::TMetaEtc& metaEtcOld,
        const TRandomId::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TRandomId::TMetaEtc& metaEtcOld,
        const TRandomId::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TSchemaTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TSchemaTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TSchema* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TSchema* object,
        const TSchema::TMetaEtc& metaEtcOld,
        const TSchema::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TSchema::TMetaEtc& metaEtcOld,
        const TSchema::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TSemaphoreTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TSemaphoreTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;
    NYT::NOrm::NClient::NObjects::TObjectTypeValue GetParentType() const override;

    const TDBFields& GetParentKeyFields() const override;

    NYT::NOrm::NServer::NObjects::TObject* GetParent(
        const NYT::NOrm::NServer::NObjects::TObject* object,
        std::source_location location = std::source_location::current()) override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const TDBFields ParentKeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TSemaphore* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TSemaphore* object,
        const TSemaphore::TMetaEtc& metaEtcOld,
        const TSemaphore::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TSemaphore::TMetaEtc& metaEtcOld,
        const TSemaphore::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TSemaphoreSetTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TSemaphoreSetTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TSemaphoreSet* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TSemaphoreSet* object,
        const TSemaphoreSet::TMetaEtc& metaEtcOld,
        const TSemaphoreSet::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TSemaphoreSet::TMetaEtc& metaEtcOld,
        const TSemaphoreSet::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TTimestampIdTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TTimestampIdTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TTimestampId* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TTimestampId* object,
        const TTimestampId::TMetaEtc& metaEtcOld,
        const TTimestampId::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TTimestampId::TMetaEtc& metaEtcOld,
        const TTimestampId::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TTypographerTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TTypographerTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;
    NYT::NOrm::NClient::NObjects::TObjectTypeValue GetParentType() const override;

    const TDBFields& GetParentKeyFields() const override;

    NYT::NOrm::NServer::NObjects::TObject* GetParent(
        const NYT::NOrm::NServer::NObjects::TObject* object,
        std::source_location location = std::source_location::current()) override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const TDBFields ParentKeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TTypographer* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TTypographer* object,
        const TTypographer::TMetaEtc& metaEtcOld,
        const TTypographer::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TTypographer::TMetaEtc& metaEtcOld,
        const TTypographer::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TUserTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TUserTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TUser* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TUser* object,
        const TUser::TMetaEtc& metaEtcOld,
        const TUser::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TUser::TMetaEtc& metaEtcOld,
        const TUser::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

class TWatchLogConsumerTypeHandler
    : public NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NExample::NServer::NPlugins::TObjectTypeHandlerBase;

public:
    TWatchLogConsumerTypeHandler(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

    void Initialize() override;
    void PostInitialize() override;

    bool IsObjectNameSupported() const override;

    bool SkipStoreWithoutChanges() const override;

    bool ForceZeroKeyEvaluation() const override;

    std::vector<NYT::NOrm::NServer::NObjects::TWatchLog> GetWatchLogs() const override;

    const NYT::NYson::TProtobufMessageType* GetRootProtobufType() const override;

    const TDBFields& GetKeyFields() const override;

    NYT::NOrm::NClient::NObjects::TObjectKey GetNullKey() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetTable() const override;

    const NYT::NOrm::NServer::NObjects::TDBTable* GetParentsTable() const override;

    std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> InstantiateObject(
        const NYT::NOrm::NClient::NObjects::TObjectKey& key,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::ISession* session) override;

    void InitializeCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

    void ValidateCreatedObject(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        NYT::NOrm::NServer::NObjects::TObject* object) override;

protected:
    const NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr Config_;
    const TDBFields KeyFields_;
    const NYT::NOrm::NClient::NObjects::TObjectKey NullKey_;

    void ValidateMetaEtcOnTransactionCommit(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TWatchLogConsumer* object);

    void ValidateMetaEtcOnValueUpdate(
        NYT::NOrm::NServer::NObjects::TTransaction* transaction,
        const TWatchLogConsumer* object,
        const TWatchLogConsumer::TMetaEtc& metaEtcOld,
        const TWatchLogConsumer::TMetaEtc& metaEtcNew);

    void ValidateMetaUuid(
        const TWatchLogConsumer::TMetaEtc& metaEtcOld,
        const TWatchLogConsumer::TMetaEtc& metaEtcNew);
};

////////////////////////////////////////////////////////////////////////////////

} // NYT::NOrm::NExample::NServer::NLibrary
