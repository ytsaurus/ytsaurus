#pragma once

#include "type_handler.h"

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TNullTypeHandler
    : public ITypeHandler
{
public:
    std::optional<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options) override;
    std::optional<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options) override;
    std::optional<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options) override;
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualTypeHandler
    : public TNullTypeHandler
{
public:
    std::optional<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options) override;
    std::optional<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options) override;
    std::optional<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options) override;

protected:
    virtual NObjectClient::EObjectType GetSupportedObjectType() = 0;
    virtual NYson::TYsonString GetObjectYson(NObjectClient::TObjectId objectId) = 0;
    virtual std::optional<NObjectClient::TObjectId> TryCreateObject(const TCreateObjectOptions& options);

private:
    std::optional<NYson::TYsonString> TryGetObjectYson(
        const NYPath::TYPath& path,
        NYPath::TYPath* pathSuffix);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

