#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TMountConfigAttributeDictionary
    : public NYTree::IAttributeDictionary
{
public:
    TMountConfigAttributeDictionary(
        NCellMaster::TBootstrap* bootstrap,
        TTableNode* owner,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* baseAttributes,
        bool includeOldAttributesInList);

    std::vector<TString> ListKeys() const override;
    std::vector<TKeyValuePair> ListPairs() const override;

    NYson::TYsonString FindYson(TStringBuf key) const override;
    void SetYson(const TString& key, const NYson::TYsonString& value) override;
    bool Remove(const TString& key) override;

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    TTableNode* const Owner_;
    NTransactionServer::TTransaction* const Transaction_;
    NYTree::IAttributeDictionary* const BaseAttributes_;
    const bool IncludeOldAttributesInList_ = false;

    TTableNode* LockMountConfigAttribute();
};

DEFINE_REFCOUNTED_TYPE(TMountConfigAttributeDictionary)

////////////////////////////////////////////////////////////////////////////////

void InternalizeMountConfigAttributes(NYTree::IAttributeDictionary* attributes);

////////////////////////////////////////////////////////////////////////////////

// COMPAT(ifsmirnov): EMasterReign::BuiltinMountConfig
std::vector<std::pair<TString, NYson::TYsonString>> ExtractOldStyleMountConfigAttributes(
    NObjectServer::TAttributeSet* attributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
