#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TImmutableMountConfigAttributeDictionary
    : public NYTree::IAttributeDictionary
{
public:
    TImmutableMountConfigAttributeDictionary(
        NCellMaster::TBootstrap* bootstrap,
        const TTableNode* owner,
        NTransactionServer::TTransaction* transaction,
        const NYTree::IAttributeDictionary* baseAttributes,
        bool includeOldAttributesInList);

    std::vector<TKey> ListKeys() const override;
    std::vector<TKeyValuePair> ListPairs() const override;

    TValue FindYson(TKeyView key) const override;
    void SetYson(TKeyView key, const TValue& value) override;
    bool Remove(TKeyView key) override;

protected:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TTableNode* const Owner_;
    NTransactionServer::TTransaction* const Transaction_;
    const NYTree::IAttributeDictionary* const BaseAttributes_;
    const bool IncludeOldAttributesInList_ = false;
};

DEFINE_REFCOUNTED_TYPE(TImmutableMountConfigAttributeDictionary)

////////////////////////////////////////////////////////////////////////////////

class TMutableMountConfigAttributeDictionary
    : public TImmutableMountConfigAttributeDictionary
{
public:
    TMutableMountConfigAttributeDictionary(
        NCellMaster::TBootstrap* bootstrap,
        TTableNode* owner,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* mutableBaseAttributes,
        bool includeOldAttributesInList);

    void SetYson(TKeyView key, const TValue& value) override;
    bool Remove(TKeyView key) override;

private:
    TTableNode* const Owner_;
    NYTree::IAttributeDictionary* const BaseAttributes_;

    TTableNode* LockMountConfigAttribute();
};

DEFINE_REFCOUNTED_TYPE(TMutableMountConfigAttributeDictionary)

////////////////////////////////////////////////////////////////////////////////

void InternalizeMountConfigAttributes(NYTree::IAttributeDictionary* attributes);

////////////////////////////////////////////////////////////////////////////////

// COMPAT(ifsmirnov): EMasterReign::BuiltinMountConfig
std::vector<std::pair<std::string, NYson::TYsonString>> ExtractOldStyleMountConfigAttributes(
    NObjectServer::TAttributeSet* attributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
