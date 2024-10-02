// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "traits.h"

#include <yt/yt/orm/client/misc/protobuf_helpers.h>

#include <yt/yt/orm/example/client/proto/data_model/autogen/schema.pb.h>

namespace NYT::NOrm::NExample::NClient::NApi {

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthor& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_id2()) {
        destination.set_id2(source.id2());
    } else {
        hasAllKeys = false;
    }
    if (source.has_publisher_id()) {
        destination.set_publisher_id(source.publisher_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id() &&
        source.has_id2();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id(),
        source.id2());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 2,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
    destination.Setid2(key.GetWithDefault<std::decay_t<decltype(destination.id2())>>(1));
}
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_publisher_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing parent keys for %v",
        source.GetTypeName());
    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.publisher_id());
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setpublisher_id(key.GetWithDefault<std::decay_t<decltype(destination.publisher_id())>>(0));
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& source,
    bool ensureAllKeysPresent)
{
    return GetParentKey(source, ensureAllKeysPresent) + GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 3,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setpublisher_id(key.GetWithDefault<std::decay_t<decltype(destination.publisher_id())>>(0));
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(1));
    destination.Setid2(key.GetWithDefault<std::decay_t<decltype(destination.id2())>>(2));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBook& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TEditor referencedObject;
        auto value = source.spec().editor_id();
        referencedObject.mutable_meta()->set_id(value);
        result[std::pair{EObjectType::Editor, "/spec/editor_id"}].push_back(GetObjectKey(referencedObject.meta()));
    }
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator referencedObject;
        auto value = source.spec().illustrator_id();
        referencedObject.mutable_meta()->set_uid(value);
        result[std::pair{EObjectType::Illustrator, "/spec/illustrator_id"}].push_back(GetObjectKey(referencedObject.meta()));
    }
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher referencedObject;
        for (const auto& value : source.spec().alternative_publisher_ids()) {
            referencedObject.mutable_meta()->set_id(value);
            result[std::pair{EObjectType::Publisher, "/spec/alternative_publisher_ids"}].push_back(GetObjectKey(referencedObject.meta()));
        }
    }
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator referencedObject;
        auto value = source.spec().cover_illustrator_id();
        referencedObject.mutable_meta()->set_uid(value);
        result[std::pair{EObjectType::Illustrator, "/spec/cover_illustrator_id"}].push_back(GetObjectKey(referencedObject.meta()));
    }
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthor referencedObject;
        for (const auto& value : source.spec().peer_review().reviewer_ids()) {
            referencedObject.mutable_meta()->set_id(value);
            result[std::pair{EObjectType::Author, "/spec/peer_review/reviewer_ids"}].push_back(GetObjectKey(referencedObject.meta()));
        }
    }
    {
        auto vals = NYT::NOrm::NClient::VectorFromProtoField(source.spec().author_ids());
        for (const auto& value : vals) {
            NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthor referencedObject;
            referencedObject.mutable_meta()->set_id(value);
            result[std::pair{EObjectType::Author, "/spec/author_ids"}].push_back(GetObjectKey(referencedObject.meta()));
        }
    }
    {
        auto vals = NYT::NOrm::NClient::VectorFromProtoField(source.status().hitchhiker_ids());
        for (const auto& value : vals) {
            NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhiker referencedObject;
            referencedObject.mutable_meta()->set_id(value);
            result[std::pair{EObjectType::Hitchhiker, "/status/hitchhiker_ids"}].push_back(GetObjectKey(referencedObject.meta()));
        }
    }
    {
        auto vals = NYT::NOrm::NClient::VectorFromProtoField(source.status().formed_hitchhiker_ids());
        for (const auto& value : vals) {
            NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhiker referencedObject;
            referencedObject.mutable_meta()->set_id(value);
            result[std::pair{EObjectType::Hitchhiker, "/status/formed_hitchhiker_ids"}].push_back(GetObjectKey(referencedObject.meta()));
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_i64_id()) {
        destination.set_i64_id(source.i64_id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_ui64_id()) {
        destination.set_ui64_id(source.ui64_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_i64_id() &&
        source.has_ui64_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.i64_id(),
        source.ui64_id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 2,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(0));
    destination.Setui64_id(key.GetWithDefault<std::decay_t<decltype(destination.ui64_id())>>(1));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 2,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(0));
    destination.Setui64_id(key.GetWithDefault<std::decay_t<decltype(destination.ui64_id())>>(1));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampId& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TCat& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TEditor& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutor& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenre& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroup& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhiker& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TBook referencedObject;
        auto value = source.meta().formative_book_id();
        referencedObject.mutable_meta()->set_id(value);
        result[std::pair{EObjectType::Book, "/meta/formative_book_id"}].push_back(GetObjectKey(referencedObject.meta()));
    }
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TBook referencedObject;
        auto value = source.meta().formative_book_id2();
        referencedObject.mutable_meta()->set_id(value);
        result[std::pair{EObjectType::Book, "/meta/formative_book_id2"}].push_back(GetObjectKey(referencedObject.meta()));
    }
    {
        auto vals = NYT::NOrm::NClient::VectorFromProtoField(source.spec().favorite_book());
        for (const auto& value : vals) {
            NYT::NOrm::NExample::NClient::NProto::NDataModel::TBook referencedObject;
            referencedObject.mutable_meta()->set_id(value.id());
            referencedObject.mutable_meta()->set_id2(value.id2());
            result[std::pair{EObjectType::Book, "/spec/favorite_book"}].push_back(GetObjectKey(referencedObject.meta()));
        }
    }
    {
        auto vals = NYT::NOrm::NClient::VectorFromProtoField(source.spec().hated_books());
        for (const auto& value : vals) {
            NYT::NOrm::NExample::NClient::NProto::NDataModel::TBook referencedObject;
            referencedObject.mutable_meta()->set_id(value.id());
            referencedObject.mutable_meta()->set_id2(value.id2());
            result[std::pair{EObjectType::Book, "/spec/hated_books"}].push_back(GetObjectKey(referencedObject.meta()));
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_uid()) {
        destination.set_uid(source.uid());
    } else {
        hasAllKeys = false;
    }
    if (source.has_publisher_id()) {
        destination.set_publisher_id(source.publisher_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_uid();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.uid());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setuid(key.GetWithDefault<std::decay_t<decltype(destination.uid())>>(0));
}
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_publisher_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing parent keys for %v",
        source.GetTypeName());
    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.publisher_id());
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setpublisher_id(key.GetWithDefault<std::decay_t<decltype(destination.publisher_id())>>(0));
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& source,
    bool ensureAllKeysPresent)
{
    return GetParentKey(source, ensureAllKeysPresent) + GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 2,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setpublisher_id(key.GetWithDefault<std::decay_t<decltype(destination.publisher_id())>>(0));
    destination.Setuid(key.GetWithDefault<std::decay_t<decltype(destination.uid())>>(1));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher referencedObject;
        auto value = source.meta().part_time_job();
        referencedObject.mutable_meta()->set_id(value);
        result[std::pair{EObjectType::Publisher, "/meta/part_time_job"}].push_back(GetObjectKey(referencedObject.meta()));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_i64_id()) {
        destination.set_i64_id(source.i64_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_i64_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.i64_id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementId& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_mother_ship_id()) {
        destination.set_mother_ship_id(source.mother_ship_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_mother_ship_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing parent keys for %v",
        source.GetTypeName());
    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.mother_ship_id());
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setmother_ship_id(key.GetWithDefault<std::decay_t<decltype(destination.mother_ship_id())>>(0));
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& source,
    bool ensureAllKeysPresent)
{
    return GetParentKey(source, ensureAllKeysPresent) + GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 2,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setmother_ship_id(key.GetWithDefault<std::decay_t<decltype(destination.mother_ship_id())>>(0));
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(1));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptor& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_str_id()) {
        destination.set_str_id(source.str_id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_i64_id()) {
        destination.set_i64_id(source.i64_id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_ui64_id()) {
        destination.set_ui64_id(source.ui64_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_str_id() &&
        source.has_i64_id() &&
        source.has_ui64_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.str_id(),
        source.i64_id(),
        source.ui64_id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 3,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setstr_id(key.GetWithDefault<std::decay_t<decltype(destination.str_id())>>(0));
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(1));
    destination.Setui64_id(key.GetWithDefault<std::decay_t<decltype(destination.ui64_id())>>(2));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 3,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setstr_id(key.GetWithDefault<std::decay_t<decltype(destination.str_id())>>(0));
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(1));
    destination.Setui64_id(key.GetWithDefault<std::decay_t<decltype(destination.ui64_id())>>(2));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualId& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_nexus_id()) {
        destination.set_nexus_id(source.nexus_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_nexus_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing parent keys for %v",
        source.GetTypeName());
    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.nexus_id());
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setnexus_id(key.GetWithDefault<std::decay_t<decltype(destination.nexus_id())>>(0));
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& source,
    bool ensureAllKeysPresent)
{
    return GetParentKey(source, ensureAllKeysPresent) + GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 2,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setnexus_id(key.GetWithDefault<std::decay_t<decltype(destination.nexus_id())>>(0));
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(1));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShip& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutor referencedObject;
        auto value = source.spec().executor_id();
        referencedObject.mutable_meta()->set_id(value);
        result[std::pair{EObjectType::Executor, "/spec/executor_id"}].push_back(GetObjectKey(referencedObject.meta()));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_str_id()) {
        destination.set_str_id(source.str_id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_i64_id()) {
        destination.set_i64_id(source.i64_id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_ui64_id()) {
        destination.set_ui64_id(source.ui64_id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_another_ui64_id()) {
        destination.set_another_ui64_id(source.another_ui64_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_str_id() &&
        source.has_i64_id() &&
        source.has_ui64_id() &&
        source.has_another_ui64_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.str_id(),
        source.i64_id(),
        source.ui64_id(),
        source.another_ui64_id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 4,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setstr_id(key.GetWithDefault<std::decay_t<decltype(destination.str_id())>>(0));
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(1));
    destination.Setui64_id(key.GetWithDefault<std::decay_t<decltype(destination.ui64_id())>>(2));
    destination.Setanother_ui64_id(key.GetWithDefault<std::decay_t<decltype(destination.another_ui64_id())>>(3));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 4,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setstr_id(key.GetWithDefault<std::decay_t<decltype(destination.str_id())>>(0));
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(1));
    destination.Setui64_id(key.GetWithDefault<std::decay_t<decltype(destination.ui64_id())>>(2));
    destination.Setanother_ui64_id(key.GetWithDefault<std::decay_t<decltype(destination.another_ui64_id())>>(3));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyId& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumns& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexus& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstance& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisherMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TEditor referencedObject;
        auto value = source.spec().editor_in_chief();
        referencedObject.mutable_meta()->set_id(value);
        result[std::pair{EObjectType::Editor, "/spec/editor_in_chief"}].push_back(GetObjectKey(referencedObject.meta()));
    }
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator referencedObject;
        auto value = source.spec().illustrator_in_chief();
        referencedObject.mutable_meta()->set_uid(value);
        result[std::pair{EObjectType::Illustrator, "/spec/illustrator_in_chief"}].push_back(GetObjectKey(referencedObject.meta()));
    }
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher referencedObject;
        auto value = source.spec().publisher_group();
        referencedObject.mutable_meta()->set_id(value);
        result[std::pair{EObjectType::Publisher, "/spec/publisher_group"}].push_back(GetObjectKey(referencedObject.meta()));
    }
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator referencedObject;
        for (const auto& value : source.spec().featured_illustrators()) {
            referencedObject.mutable_meta()->set_uid(value);
            result[std::pair{EObjectType::Illustrator, "/spec/featured_illustrators"}].push_back(GetObjectKey(referencedObject.meta()));
        }
    }
    {
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator referencedObject;
        for (const auto& value : source.status().featured_illustrators()) {
            referencedObject.mutable_meta()->set_uid(value);
            result[std::pair{EObjectType::Illustrator, "/status/featured_illustrators"}].push_back(GetObjectKey(referencedObject.meta()));
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_str_id()) {
        destination.set_str_id(source.str_id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_i64_id()) {
        destination.set_i64_id(source.i64_id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_ui64_id()) {
        destination.set_ui64_id(source.ui64_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_str_id() &&
        source.has_i64_id() &&
        source.has_ui64_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.str_id(),
        source.i64_id(),
        source.ui64_id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 3,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setstr_id(key.GetWithDefault<std::decay_t<decltype(destination.str_id())>>(0));
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(1));
    destination.Setui64_id(key.GetWithDefault<std::decay_t<decltype(destination.ui64_id())>>(2));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 3,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setstr_id(key.GetWithDefault<std::decay_t<decltype(destination.str_id())>>(0));
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(1));
    destination.Setui64_id(key.GetWithDefault<std::decay_t<decltype(destination.ui64_id())>>(2));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomId& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchema& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_semaphore_set_id()) {
        destination.set_semaphore_set_id(source.semaphore_set_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_semaphore_set_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing parent keys for %v",
        source.GetTypeName());
    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.semaphore_set_id());
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setsemaphore_set_id(key.GetWithDefault<std::decay_t<decltype(destination.semaphore_set_id())>>(0));
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& source,
    bool ensureAllKeysPresent)
{
    return GetParentKey(source, ensureAllKeysPresent) + GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 2,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setsemaphore_set_id(key.GetWithDefault<std::decay_t<decltype(destination.semaphore_set_id())>>(0));
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(1));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphore& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSet& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_i64_id()) {
        destination.set_i64_id(source.i64_id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_ui64_id()) {
        destination.set_ui64_id(source.ui64_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_i64_id() &&
        source.has_ui64_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.i64_id(),
        source.ui64_id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 2,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(0));
    destination.Setui64_id(key.GetWithDefault<std::decay_t<decltype(destination.ui64_id())>>(1));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 2,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Seti64_id(key.GetWithDefault<std::decay_t<decltype(destination.i64_id())>>(0));
    destination.Setui64_id(key.GetWithDefault<std::decay_t<decltype(destination.ui64_id())>>(1));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampId& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }
    if (source.has_publisher_id()) {
        destination.set_publisher_id(source.publisher_id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}
NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_publisher_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing parent keys for %v",
        source.GetTypeName());
    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.publisher_id());
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setpublisher_id(key.GetWithDefault<std::decay_t<decltype(destination.publisher_id())>>(0));
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& source,
    bool ensureAllKeysPresent)
{
    return GetParentKey(source, ensureAllKeysPresent) + GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 2,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setpublisher_id(key.GetWithDefault<std::decay_t<decltype(destination.publisher_id())>>(0));
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(1));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographer& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TUser& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractObjectMetaKeyFields(const NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& source, NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& destination)
{
    bool hasAllKeys = true;
    if (source.has_id()) {
        destination.set_id(source.id());
    } else {
        hasAllKeys = false;
    }

    return hasAllKeys;
}

NYT::NOrm::NClient::NObjects::TObjectKey GetObjectKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& source,
    bool ensureAllKeysPresent)
{
    bool hasAllKeys =
        source.has_id();
    THROW_ERROR_EXCEPTION_IF(ensureAllKeysPresent && !hasAllKeys,
        "Missing keys for %v",
        source.GetTypeName());

    return NYT::NOrm::NClient::NObjects::TObjectKey(
        source.id());
}

void SetObjectKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(0u != key.size() && key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

void TrySetParentKey(
    [[ maybe_unused ]] NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& destination,
    [[ maybe_unused ]] const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
}

NYT::NOrm::NClient::NObjects::TObjectKey GetPrimaryKey(
    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& source,
    bool ensureAllKeysPresent)
{
    return GetObjectKey(source, ensureAllKeysPresent);
}

void SetPrimaryKey(
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerMeta& destination,
    const NYT::NOrm::NClient::NObjects::TObjectKey& key)
{
    THROW_ERROR_EXCEPTION_IF(key.size() != 1,
        "Missing keys for %v",
        destination.GetTypeName());
    destination.Setid(key.GetWithDefault<std::decay_t<decltype(destination.id())>>(0));
}

THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> GetReferences(
    [[maybe_unused]] const NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumer& source)
{
    THashMap<std::pair<EObjectType, NYT::NYPath::TYPath>, std::vector<NYT::NOrm::NClient::NObjects::TObjectKey>> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NApi
