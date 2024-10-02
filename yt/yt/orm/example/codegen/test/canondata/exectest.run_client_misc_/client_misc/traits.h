// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include "enums.h"

#include <yt/yt/orm/client/objects/key.h>

#include <yt/yt/orm/library/mpl/types.h>

#include <util/generic/strbuf.h>

#include <array>

namespace NYT::NOrm::NExample::NClient::NProto::NDataModel {

////////////////////////////////////////////////////////////////////////////////

class TAuthor;
class TAuthorMeta;
class TBook;
class TBookMeta;
class TBufferedTimestampId;
class TBufferedTimestampIdMeta;
class TCat;
class TCatMeta;
class TEditor;
class TSomeMeta;
class TEmployer;
class TEmployer_TMeta;
class TExecutor;
class TExecutorMeta;
class TGenre;
class TGenreMeta;
class TGroup;
class TGroupMeta;
class THitchhiker;
class THitchhikerMeta;
class TIllustrator;
class TIllustratorMeta;
class TIndexedIncrementId;
class TIndexedIncrementIdMeta;
class TInterceptor;
class TInterceptorMeta;
class TManualId;
class TManualIdMeta;
class TMotherShip;
class TMotherShipMeta;
class TMultipolicyId;
class TMultipolicyIdMeta;
class TNestedColumns;
class TNestedColumnsMeta;
class TNexus;
class TNexusMeta;
class TNirvanaDMProcessInstance;
class TNirvanaDMProcessInstanceMeta;
class TPublisher;
class TPublisherMeta;
class TRandomId;
class TRandomIdMeta;
class TSchema;
class TSchemaMeta;
class TSemaphore;
class TSemaphoreMeta;
class TSemaphoreSet;
class TSemaphoreSetMeta;
class TTimestampId;
class TTimestampIdMeta;
class TTypographer;
class TTypographerMeta;
class TUser;
class TUserMeta;
class TWatchLogConsumer;
class TWatchLogConsumerMeta;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NProto::NDataModel

namespace NYT::NOrm::NExample::NClient::NApi {

////////////////////////////////////////////////////////////////////////////////

using TObjectTypes = ::NYT::NOrm::NMpl::TTypes<
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthor,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBook,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampId,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TCat,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TEditor,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutor,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenre,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroup,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhiker,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementId,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptor,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualId,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShip,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyId,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumns,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexus,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstance,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomId,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchema,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphore,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSet,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampId,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographer,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TUser,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumer>;

template <class T>
concept CObjectType = ::NYT::NOrm::NMpl::COneOfTypes<T, TObjectTypes>;

////////////////////////////////////////////////////////////////////////////////

template <CObjectType T>
struct TObjectTypeTraitsByType;

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <EObjectType Value>
struct TObjectTypeValueComparator
{
    template <class Trait>
    constexpr bool operator()() const
    {
        return Value == Trait::Value;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <EObjectType Value>
using TObjectTypeTraitsByValue = TObjectTypes::Map<TObjectTypeTraitsByType>::
    template SuchThat<NDetail::TObjectTypeValueComparator<Value>>;

template <EObjectType Value>
using TObjectTypeByValue = typename TObjectTypeTraitsByValue<Value>::Type;

template <CObjectType T>
inline constexpr EObjectType ObjectTypeValueByType = TObjectTypeTraitsByType<T>::Value;

////////////////////////////////////////////////////////////////////////////////

//! Helper to get ObjectType by runtime enum type.
//! TInvokable must have signature `void <typename T>()`,
template <::NYT::NOrm::NMpl::CInvocableForEachType<TObjectTypes> TInvokable>
bool InvokeForObjectType(EObjectType objectType, TInvokable&& consumer)
{
    bool found = false;
    TObjectTypes::ForEach([&] <CObjectType T> {
        if (TObjectTypeTraitsByType<T>::Value == objectType) {
            found = true;
            consumer.template operator()<T>();
        }
    });
    return found;
}

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthor>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthor;

    static constexpr EObjectType Value = EObjectType::Author;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = true;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthor& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TBook>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TBook;

    static constexpr EObjectType Value = EObjectType::Book;
    static constexpr bool HasParent = true;
    using ParentType = NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher;
    static constexpr EObjectType ParentValue = EObjectType::Publisher;

    static constexpr bool HasHashColumn = true;

    static constexpr bool HasReferences = true;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/publisher_id",
        "/meta/id",
        "/meta/id2",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& source,
    bool ensureAllKeysPresent = false);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBook& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampId>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampId;

    static constexpr EObjectType Value = EObjectType::BufferedTimestampId;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/i64_id",
        "/meta/ui64_id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampId& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TCat>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TCat;

    static constexpr EObjectType Value = EObjectType::Cat;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TCat& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TEditor>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TEditor;

    static constexpr EObjectType Value = EObjectType::Editor;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TEditor& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer;

    static constexpr EObjectType Value = EObjectType::Employer;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutor>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutor;

    static constexpr EObjectType Value = EObjectType::Executor;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutor& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenre>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenre;

    static constexpr EObjectType Value = EObjectType::Genre;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenre& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroup>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroup;

    static constexpr EObjectType Value = EObjectType::Group;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = true;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroup& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhiker>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhiker;

    static constexpr EObjectType Value = EObjectType::Hitchhiker;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = true;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhiker& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator;

    static constexpr EObjectType Value = EObjectType::Illustrator;
    static constexpr bool HasParent = true;
    using ParentType = NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher;
    static constexpr EObjectType ParentValue = EObjectType::Publisher;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = true;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/publisher_id",
        "/meta/uid",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& source,
    bool ensureAllKeysPresent = false);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementId>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementId;

    static constexpr EObjectType Value = EObjectType::IndexedIncrementId;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/i64_id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementId& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptor>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptor;

    static constexpr EObjectType Value = EObjectType::Interceptor;
    static constexpr bool HasParent = true;
    using ParentType = NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShip;
    static constexpr EObjectType ParentValue = EObjectType::MotherShip;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/mother_ship_id",
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& source,
    bool ensureAllKeysPresent = false);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptor& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualId>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualId;

    static constexpr EObjectType Value = EObjectType::ManualId;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/str_id",
        "/meta/i64_id",
        "/meta/ui64_id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualId& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShip>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShip;

    static constexpr EObjectType Value = EObjectType::MotherShip;
    static constexpr bool HasParent = true;
    using ParentType = NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexus;
    static constexpr EObjectType ParentValue = EObjectType::Nexus;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = true;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/nexus_id",
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& source,
    bool ensureAllKeysPresent = false);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShip& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyId>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyId;

    static constexpr EObjectType Value = EObjectType::MultipolicyId;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/str_id",
        "/meta/i64_id",
        "/meta/ui64_id",
        "/meta/another_ui64_id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyId& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumns>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumns;

    static constexpr EObjectType Value = EObjectType::NestedColumns;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumns& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexus>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexus;

    static constexpr EObjectType Value = EObjectType::Nexus;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexus& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstance>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstance;

    static constexpr EObjectType Value = EObjectType::NirvanaDMProcessInstance;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstance& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher;

    static constexpr EObjectType Value = EObjectType::Publisher;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = true;

    static constexpr bool HasReferences = true;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomId>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomId;

    static constexpr EObjectType Value = EObjectType::RandomId;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/str_id",
        "/meta/i64_id",
        "/meta/ui64_id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomId& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchema>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchema;

    static constexpr EObjectType Value = EObjectType::Schema;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = true;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchema& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphore>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphore;

    static constexpr EObjectType Value = EObjectType::Semaphore;
    static constexpr bool HasParent = true;
    using ParentType = NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSet;
    static constexpr EObjectType ParentValue = EObjectType::SemaphoreSet;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/semaphore_set_id",
        "/meta/id",
    });

    static constexpr bool IsBuiltin = true;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& source,
    bool ensureAllKeysPresent = false);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphore& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSet>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSet;

    static constexpr EObjectType Value = EObjectType::SemaphoreSet;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = true;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSet& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampId>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampId;

    static constexpr EObjectType Value = EObjectType::TimestampId;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/i64_id",
        "/meta/ui64_id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampId& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographer>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographer;

    static constexpr EObjectType Value = EObjectType::Typographer;
    static constexpr bool HasParent = true;
    using ParentType = NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher;
    static constexpr EObjectType ParentValue = EObjectType::Publisher;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/publisher_id",
        "/meta/id",
    });

    static constexpr bool IsBuiltin = false;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& source,
    bool ensureAllKeysPresent = false);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographer& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TUser>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TUser;

    static constexpr EObjectType Value = EObjectType::User;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = true;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TUser& object);

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectTypeTraitsByType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumer>
{
    using Type = NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumer;

    static constexpr EObjectType Value = EObjectType::WatchLogConsumer;
    static constexpr bool HasParent = false;

    static constexpr bool HasHashColumn = false;

    static constexpr bool HasReferences = false;

    static constexpr auto PrimaryKeySelector = std::to_array<TStringBuf>({
        "/meta/id",
    });

    static constexpr bool IsBuiltin = true;
};

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& destination);

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& source,
    bool ensureAllKeysPresent = false);

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

void TrySetParentKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

// ParentKey (if present) + ObjectKey.
NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& source,
    bool ensureAllKeysPresent = false);

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key);

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumer& object);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NApi
