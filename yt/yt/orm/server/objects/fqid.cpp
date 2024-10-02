#include "fqid.h"

#include "attribute_policy.h"
#include "attribute_schema.h"
#include "key_util.h"
#include "object.h"
#include "object_manager.h"
#include "private.h"
#include "type_handler.h"

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/helpers.h>

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/library/query/base/helpers.h>

#include <library/cpp/string_utils/quote/quote.h>

#include <util/string/split.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NClient::NObjects;

////////////////////////////////////////////////////////////////////////////////

TFqid::TFqid(NMaster::IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , DBName_(bootstrap->GetDBName())
{ }

TString TFqid::Serialize() const
{
    TString key = Key_.ToString();
    // We may put an FQID inside an FQID, by way of having an FQID key field (e.g., relation).
    // We need to escape the |'s. We'll use the standard CGI escaping with %XX, but we won't escape
    // some common characters (including all currently allowed in object ids).
    Quote(key, "&+:=");
    return Format("%v|%v|%v|%v|%v",
        Bootstrap_->GetServiceName(),
        DBName(),
        Schema(),
        key,
        Uuid());
}

void TFqid::Validate() const
{
    NMaster::ValidateDbName(DBName());

    if (Type() == TObjectTypeValues::Null) {
        THROW_ERROR_EXCEPTION("Unsupported object type %Qv",
            Type());
    }

    const auto& objectManager = Bootstrap_->GetObjectManager();
    if (objectManager) { // optional for unit tests
        ValidateObjectKey(objectManager->GetTypeHandlerOrThrow(Type()), Key_);
    }

    static const auto UuidValidator = CreateStringAttributePolicy(
        EAttributeGenerationPolicy::Manual,
        7, 35,
        "0123456789abcdef-");
    UuidValidator->Validate(Uuid(), "Uuid");
}

TFqid TFqid::Parse(NMaster::IBootstrap* bootstrap, TStringBuf serialized)
{
    std::vector<TStringBuf> tokens = StringSplitter(serialized).Split(FqidSeparator);
    if (tokens.size() != 5) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidObjectId,
            "Fqid %v contains %v fields, expected 5",
            serialized,
            tokens.size());
    }

    if (tokens[0] != bootstrap->GetServiceName()) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidObjectId,
            "Fqid %v is from a foreign service and cannot be parsed by %v)",
            serialized,
            bootstrap->GetServiceName());
    }

    TFqid fqid(bootstrap);
    fqid.DBName(TString(tokens[1])).Schema(tokens[2]).Uuid(TString(tokens[4]));

    TString serializedKey = UrlUnescapeRet(tokens[3]);
    const auto& objectManager = bootstrap->GetObjectManager();
    if (objectManager) { // optional for unit tests
        const auto& dbFields = objectManager->GetTypeHandlerOrThrow(fqid.Type())->GetKeyFields();
        fqid.Key(ParseObjectKey(serializedKey, dbFields));
    } else {
        fqid.Key(TObjectKey(serializedKey));
    }

    return fqid;
}

const TString& TFqid::DBName() const
{
    return DBName_;
}

TObjectTypeValue TFqid::Type() const
{
    return Type_;
}

TObjectId TFqid::Schema() const
{
    return NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(Type());
}

const TObjectKey& TFqid::Key() const
{
    return Key_;
}

const TObjectId& TFqid::Uuid() const
{
    return Uuid_;
}

TFqid& TFqid::DBName(TString dbName)
{
    DBName_ = std::move(dbName);
    return *this;
}

TFqid& TFqid::Type(TObjectTypeValue type)
{
    Type_ = type;
    return *this;
}

TFqid& TFqid::Schema(TStringBuf schema)
{
    Type(NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeValueByNameOrThrow(TObjectTypeName(schema)));
    return *this;
}

TFqid& TFqid::Key(TObjectKey key)
{
    Key_ = std::move(key);
    return *this;
}

TFqid& TFqid::Uuid(TObjectId uuid)
{
    Uuid_ = std::move(uuid);
    return *this;
}

TFqid& TFqid::Uuid(const NProto::TMetaEtc& metaEtc)
{
    Uuid(metaEtc.uuid());
    return *this;
}

NQueryClient::NAst::TExpressionPtr BuildFqidExpression(
    const IObjectTypeHandler* typeHandler,
    IQueryContext* context)
{
    const auto& service = typeHandler->GetBootstrap()->GetServiceName();
    const auto& dbName = typeHandler->GetBootstrap()->GetDBName();
    const auto& type = GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(typeHandler->GetType());

    auto fqidSeparator = TString(FqidSeparator);
    // '%3B' is encode for ';', more info in "fqid.h".
    auto keySeparator = TString("%3B");

    auto fqidBegin = context->New<NQueryClient::NAst::TLiteralExpression>(
        NQueryClient::TSourceLocation(),
        service + fqidSeparator + dbName + fqidSeparator + type);
    auto keyExpression = BuildKeyExpression(
        typeHandler->GetKeyFields(),
        typeHandler->GetIdAttributeSchemas(),
        keySeparator,
        context);

    auto uuid = typeHandler->GetUuidLocation();
    auto uuidExpression = uuid.Attribute->AsScalar()->RunExpressionBuilder(
        context,
        uuid.SuffixPath,
        EAttributeExpressionContext::Fetch);

    return NQueryClient::BuildConcatenationExpression(
        context,
        fqidBegin,
        NQueryClient::BuildConcatenationExpression(
            context,
            keyExpression,
            uuidExpression,
            fqidSeparator),
        fqidSeparator);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
