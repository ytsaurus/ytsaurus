#pragma once

#include "public.h"

#include <yt/client/scheduler/public.h>

#include <yt/client/formats/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TFormatManager
{
public:
    TFormatManager(THashMap<NFormats::EFormatType, TFormatConfigPtr> formatConfigs, TString authenticatedUser);

    NFormats::TFormat ConvertToFormat(const NYTree::INodePtr& formatNode, const TString& origin) const;

    void ValidateAndPatchOperationSpec(
        const NYTree::INodePtr& specNode,
        NScheduler::EOperationType operationType) const;

private:
    THashMap<NFormats::EFormatType, TFormatConfigPtr> FormatConfigs_;
    TString AuthenticatedUser_;

private:
    void ValidateAndPatchFormatNode(const NYTree::INodePtr& formatNode, const TString& origin) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
