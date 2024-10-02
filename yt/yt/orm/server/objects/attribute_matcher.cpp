#include "attribute_matcher.h"

#include "attribute_schema.h"
#include "fqid.h"
#include "key_util.h"
#include "object_manager.h"
#include "type_handler.h"

#include <yt/yt/orm/server/master/bootstrap.h>

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/client/objects/registry.h>

#include <util/generic/algorithm.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// Implements common functionality of co-traversing yson and schemas and handling keys.
// Defers task-specific details via virtuals.
class TBaseMatcher
{
public:
    explicit TBaseMatcher(const IObjectTypeHandler* typeHandler)
        : TypeHandler_(typeHandler)
    { }

    void MatchRecursively(const INodePtr& node, const TAttributeSchema* schema)
    {
        if (node == nullptr) {
            return;
        }

        TrySetMetaNode(node, schema);

        if (auto* compositeSchema = schema->TryAsComposite()) {
            if (node->GetType() != ENodeType::Map) {
                THROW_ERROR_EXCEPTION(
                    "Attribute %Qv is composite and cannot be parsed from a %Qlv node",
                    compositeSchema->FormatPathEtc(),
                    node->GetType());
            }

            auto mapNode = node->AsMap();
            for (const auto& [key, childNode] : mapNode->GetChildren()) {
                if (auto* childSchema = compositeSchema->FindChild(key); childSchema) {
                    MatchRecursively(childNode, childSchema);
                } else if (auto* etcSchema = compositeSchema->FindEtcChildByFieldName(key); etcSchema) {
                    MatchEtc(childNode, key, etcSchema);
                    OnScalarSchemaMatched(etcSchema, key);
                } else if (auto* defaultEtcSchema = compositeSchema->GetEtcChild(); defaultEtcSchema) {
                    MatchEtc(childNode, key, defaultEtcSchema);
                    OnScalarSchemaMatched(defaultEtcSchema, key);
                } else {
                    THROW_ERROR_EXCEPTION("Attribute %Qv has no child with key %Qv",
                        schema->FormatPathEtc(),
                        key);
                }
            }
        } else {
            auto* scalarSchema = schema->TryAsScalar();
            YT_VERIFY(scalarSchema);

            bool matched =
                TryMatchKey(node, scalarSchema) ||
                TryMatchKeyField(node, scalarSchema) ||
                TryMatchParentKey(node, scalarSchema) ||
                TryMatchParentKeyField(node, scalarSchema);

            if (!matched) {
                MatchAttribute(node, scalarSchema);
            }

            OnScalarSchemaMatched(scalarSchema);
        }
    }

protected:
    const IObjectTypeHandler* TypeHandler_;
    INodePtr MetaNode_;

    virtual void OnScalarSchemaMatched(const TScalarAttributeSchema* /*schema*/, std::string_view /*key*/ = "")
    { }

    virtual void MatchEtc(
        const INodePtr& node,
        std::string_view key,
        const TScalarAttributeSchema* schema) = 0;

    virtual void MatchKey(const INodePtr& /*node*/)
    { }

    virtual void MatchKeyField(const INodePtr& /*node*/, size_t /*index*/)
    { }

    virtual void MatchParentKey(const INodePtr& /*node*/)
    { }

    virtual void MatchParentKeyField(const INodePtr& /*node*/, size_t /*index*/)
    { }

    virtual void MatchAttribute(const INodePtr& /*node*/, const TScalarAttributeSchema* /*schema*/)
    { }

    bool TryMatchKey(const INodePtr& node, const TScalarAttributeSchema* schema)
    {
        if (schema->GetPath() != "/meta/key") {
            return false;
        }
        MatchKey(node);
        return true;
    }

    bool TryMatchKeyField(const INodePtr& node, const TScalarAttributeSchema* schema)
    {
        size_t index = FindIndex(TypeHandler_->GetIdAttributeSchemas(), schema);
        if (index == NPOS) {
            return false;
        }
        MatchKeyField(node, index);
        return true;
    }

    bool TryMatchParentKey(const INodePtr& node, const TScalarAttributeSchema* schema)
    {
        if (schema->GetPath() != "/meta/parent_key") {
            return false;
        }
        MatchParentKey(node);
        return true;
    }

    bool TryMatchParentKeyField(const INodePtr& node, const TScalarAttributeSchema* schema)
    {
        size_t index = FindIndex(TypeHandler_->GetParentIdAttributeSchemas(), schema);
        if (index == NPOS) {
            return false;
        }
        MatchParentKeyField(node, index);
        return true;
    }

    void TrySetMetaNode(const INodePtr& node, const TAttributeSchema* schema)
    {
        if (MetaNode_) {
            return;
        }
        if (schema->GetPath() == "/meta") {
            MetaNode_ = node;
        }
    }
}; // TBaseMatcher

////////////////////////////////////////////////////////////////////////////////

class TCreationMatcher
    : public TBaseMatcher
{
public:
    TCreationAttributeMatches Result;

    explicit TCreationMatcher(const IObjectTypeHandler* typeHandler)
        : TBaseMatcher(typeHandler)
    {
        typeHandler->GetRootAttributeSchema()->ForEachLeafAttribute([this] (const TScalarAttributeSchema* schema) {
            for (const auto& key : schema->GetMandatory()) {
                EmplaceOrCrash(Result.UnmatchedMandatoryAttributes, schema, key);
            }
            if (schema->HasInitializer()) {
                InsertOrCrash(Result.PendingInitializerAttributes, schema);
            }

            return false;
        });
    }

protected:
    void MatchEtc(const INodePtr& node, std::string_view key, const TScalarAttributeSchema* schema) override
    {
        if (schema->GetPath() == "/meta" && key == "uuid") {
            TypeHandler_
                ->GetBootstrap()
                ->GetAccessControlManager()
                ->ValidateSuperuser("set /meta/uuid");
        }
        AddMatch({schema, TSetUpdateRequest{.Path = "/" + NYPath::ToYPathLiteral(key), .Value = node}});
    }

    void MatchAttribute(const INodePtr& node, const TScalarAttributeSchema* schema) override
    {
        if (schema->GetPath() == "/meta/uuid") {
            TypeHandler_
                ->GetBootstrap()
                ->GetAccessControlManager()
                ->ValidateSuperuser("set /meta/uuid");
        }
        if (!schema->HasValueSetter()) {
            THROW_ERROR_EXCEPTION("Attribute %Qv cannot be set", schema->FormatPathEtc());
        }
        AddMatch({schema, TSetUpdateRequest{.Path = TYPath(), .Value = node}});
    }

    void OnScalarSchemaMatched(const TScalarAttributeSchema* schema, std::string_view key) override
    {
        // Mandatory empty key on etc requires arbitrary subpath of etc
        // to be set. That provides mandatory support for protobuf oneof.
        if (!key.empty() && schema->IsEtc()) {
            OnScalarSchemaMatched(schema, /*key*/ {});
        }

        if (auto it = Result.UnmatchedMandatoryAttributes.find(std::pair{schema, key});
            it != Result.UnmatchedMandatoryAttributes.end())
        {
            Result.UnmatchedMandatoryAttributes.erase(it);
        }
    }

    void AddMatch(TAttributeUpdateMatch match)
    {
        Result.PendingInitializerAttributes.erase(match.Schema);
        Result.Matches.push_back(std::move(match));
    }
}; // TCreationMatcher

////////////////////////////////////////////////////////////////////////////////

class TKeyMatcher
    : public TBaseMatcher
{
public:
    std::vector<NYTree::INodePtr> MatchedKeyAttributes;
    std::vector<NYTree::INodePtr> MatchedParentKeyAttributes;
    bool AutogenerateKey = false;
    TTransaction* Transaction = nullptr;
    TKeyAttributeMatches Result;

    explicit TKeyMatcher(const IObjectTypeHandler* typeHandler)
        : TBaseMatcher(typeHandler)
    {
        MatchedKeyAttributes.resize(typeHandler->GetIdAttributeSchemas().size());
        MatchedParentKeyAttributes.resize(typeHandler->GetParentIdAttributeSchemas().size());
    }

    void Finalize()
    {
        bool forceZeroKeyEvaluation = TypeHandler_->ForceZeroKeyEvaluation();
        SetKey(
            Result.Key,
            BuildKey(
                TypeHandler_->GetKeyFields(),
                TypeHandler_->GetIdAttributeSchemas(),
                MatchedKeyAttributes,
                AutogenerateKey && !Result.Key,
                forceZeroKeyEvaluation));

        bool forceParentZeroKeyEvaluation = false;
        if (TypeHandler_->HasParent()) {
            const auto* parentTypeHandler = TypeHandler_->GetBootstrap()
                ->GetObjectManager()
                ->GetTypeHandlerOrCrash(TypeHandler_->GetParentType());
            if (parentTypeHandler) {
                forceParentZeroKeyEvaluation = parentTypeHandler->ForceZeroKeyEvaluation();
            }
        }

        SetKey(
            Result.ParentKey,
            BuildKey(
                TypeHandler_->GetParentKeyFields(),
                TypeHandler_->GetParentIdAttributeSchemas(),
                MatchedParentKeyAttributes,
                /*autogenerate*/ false,
                forceParentZeroKeyEvaluation));
    }

protected:
    void MatchEtc(const INodePtr& node, std::string_view key, const TScalarAttributeSchema* schema) override
    {
        if (schema->GetPath() == "/meta" && key == "uuid") {
            SetUuid(node->AsString()->GetValue());
        }
    }

    void MatchKey(const INodePtr& node) override
    {
        SetKey(Result.Key, ParseObjectKey(
            node->AsString()->GetValue(),
            TypeHandler_->GetKeyFields()));
    }

    void MatchKeyField(const INodePtr& node, size_t index) override
    {
        MatchedKeyAttributes[index] = node;
    }

    void MatchParentKey(const INodePtr& node) override
    {
        SetKey(Result.ParentKey, ParseObjectKey(
            node->AsString()->GetValue(),
            TypeHandler_->GetParentKeyFields()));
    }

    void MatchParentKeyField(const INodePtr& node, size_t index) override
    {
        MatchedParentKeyAttributes[index] = node;
    }

    void MatchAttribute(const INodePtr& node, const TScalarAttributeSchema* schema) override
    {
        if (schema->GetPath() == "/meta/fqid") {
            OnMetaFqid(node->AsString()->GetValue());
        }
    }

    void OnMetaFqid(TString serializedFqid)
    {
        auto bootstrap = TypeHandler_->GetBootstrap();
        auto fqid = TFqid::Parse(bootstrap, serializedFqid);
        fqid.Validate();
        if (fqid.DBName() != bootstrap->GetDBName()) {
            THROW_ERROR_EXCEPTION("Fqid %v is from a different database; this is %v",
                serializedFqid,
                bootstrap->GetDBName());
        }
        if (fqid.Type() != TypeHandler_->GetType()) {
            THROW_ERROR_EXCEPTION("Fqid %v requested for the wrong object type %v",
                serializedFqid,
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TypeHandler_->GetType()));
        }
        SetKey(Result.Key, fqid.Key());
        SetUuid(fqid.Uuid());
    }

    void SetUuid(TObjectId newUuid)
    {
        if (!newUuid) {
            return;
        }
        auto& oldUuid = Result.Uuid;
        if (oldUuid) {
            if (oldUuid == newUuid) {
                return;
            }
            THROW_ERROR_EXCEPTION("Uuid mismatch (%v and %v)", oldUuid, newUuid);
        }
        oldUuid = std::move(newUuid);
    }

    static void SetKey(TObjectKey& oldKey, TObjectKey newKey)
    {
        if (!newKey) {
            return;
        }
        if (oldKey) {
            if (oldKey == newKey) {
                return;
            }
            THROW_ERROR_EXCEPTION("Key mismatch (%v and %v)", oldKey, newKey);
        }
        oldKey = std::move(newKey);
    }

    TObjectKey BuildKey(
        const std::vector<const TDBField*>& dbFields,
        const IObjectTypeHandler::TScalarAttributeSchemas& attributeSchemas,
        const std::vector<NYTree::INodePtr>& matchedAttributes,
        bool autogenerate,
        bool forceZeroKeyEvaluation)
    {
        int count = std::ssize(dbFields);
        YT_VERIFY(count == std::ssize(attributeSchemas));

        TObjectKey::TKeyFields keyFields;
        keyFields.resize(count);

        TCompactVector<int, 4> unsetKeyFields{};
        for (int index = 0; index != count; ++index) {
            const auto& schema = attributeSchemas[index];
            const auto& dbField = dbFields[index];
            const auto& match = matchedAttributes[index];

            auto evaluatedField = schema->TryEvaluateKeyField(MetaNode_);
            if (match) {
                auto parsed = ParseObjectKeyField(match, dbField->Type);
                if (evaluatedField) {
                    if (parsed == evaluatedField.value() || (parsed.IsZero() && forceZeroKeyEvaluation)) {
                        keyFields[index] = std::move(evaluatedField.value());
                    } else {
                        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                            "Evaluated value %v for key field %Qv does not match with existent %v",
                            evaluatedField.value(),
                            dbField->Name,
                            parsed);
                    }
                } else {
                    keyFields[index] = std::move(parsed);
                }
                schema->ValidateKeyField(keyFields[index]);
            } else if (evaluatedField) {
                keyFields[index] = std::move(evaluatedField.value());
            } else if (autogenerate) {
                THROW_ERROR_EXCEPTION_UNLESS(Transaction,
                    "Cannot autogenerate key without a transaction");
                keyFields[index] = schema->GenerateKeyField(Transaction);
            } else {
                unsetKeyFields.push_back(index);
            }
        }
        if (std::ssize(unsetKeyFields) == count) {
            return {};
        } else if (!unsetKeyFields.empty()) {
            THROW_ERROR_EXCEPTION("Missing values for key fields: %Qv",
                MakeFormattableView(unsetKeyFields, [&] (auto* builder, const auto& unsetField) {
                    builder->AppendFormat("%v", dbFields[unsetField]->Name);
                }));
        }
        return TObjectKey(std::move(keyFields));
    }
}; // TKeyMatcher

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TCreationAttributeMatches MatchCreationAttributes(
    const IObjectTypeHandler* typeHandler,
    const NYTree::INodePtr& attributes)
{
    NDetail::TCreationMatcher matcher(typeHandler);
    matcher.MatchRecursively(attributes, typeHandler->GetRootAttributeSchema());
    return std::move(matcher.Result);
}

TKeyAttributeMatches MatchKeyAttributes(
    const IObjectTypeHandler* typeHandler,
    const NYTree::INodePtr& attributes,
    TTransaction* transaction,
    bool autogenerateKey)
{
    NDetail::TKeyMatcher matcher(typeHandler);
    matcher.Transaction = transaction;
    matcher.AutogenerateKey = autogenerateKey;
    matcher.MatchRecursively(attributes, typeHandler->GetMetaAttributeSchema());
    matcher.Finalize();
    return std::move(matcher.Result);
}

void FormatValue(TStringBuilderBase* builder, const TKeyAttributeMatches& match, TStringBuf)
{
    builder->AppendFormat("{Key: %v, ParentKey: %v, Uuid: %v}",
        match.Key,
        match.ParentKey,
        match.Uuid);
}

} // namespace NYT::NOrm::NServer::NObjects
