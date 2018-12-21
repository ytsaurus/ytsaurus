#pragma once

#include "public.h"

#include "value.h"

#include <yt/core/ytree/public.h>

namespace NYT::NClickHouseServer::NNative {

////////////////////////////////////////////////////////////////////////////////

using TDocumentPath = std::vector<TString>;
using TDocumentKeys = std::vector<TString>;

////////////////////////////////////////////////////////////////////////////////

/// Container for configurations

struct IDocument
{
    virtual ~IDocument() = default;

    virtual bool Has(const TDocumentPath& path) const = 0;
    virtual IDocumentPtr GetSubDocument(const TDocumentPath& path) const = 0;

    virtual TValue AsValue() const = 0;

    TValue GetValue(const TDocumentPath& path) const
    {
        return GetSubDocument(path)->AsValue();
    }

    virtual TDocumentKeys ListKeys() const = 0;

    TDocumentKeys ListKeys(const TDocumentPath& path) const
    {
        return GetSubDocument(path)->ListKeys();
    }

    virtual bool IsComposite() const = 0;
    virtual TString Serialize() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDocumentPtr CreateDocument(NYTree::INodePtr documentNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
