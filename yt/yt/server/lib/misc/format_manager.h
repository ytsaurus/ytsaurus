#pragma once

#include "public.h"

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/client/formats/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TFormatManager
{
public:
    TFormatManager(THashMap<NFormats::EFormatType, TFormatConfigPtr> formatConfigs, TString authenticatedUser);

    NFormats::TFormat ConvertToFormat(const NYTree::INodePtr& formatNode, TString origin) const;

    void ValidateAndPatchOperationSpec(
        const NYTree::INodePtr& specNode,
        NScheduler::EOperationType operationType) const;

    void ValidateAndPatchFormatNode(const NYTree::INodePtr& formatNode, TString origin) const;

private:
    THashMap<NFormats::EFormatType, TFormatConfigPtr> FormatConfigs_;
    TString AuthenticatedUser_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
