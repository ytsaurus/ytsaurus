#include "fn_attributes_ops.h"
#include <yt/cpp/roren/interface/fns.h>


namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void TFnAttributesOps::Merge(TFnAttributes& destination, const TFnAttributes& source)
{
    destination.IsPure_ |= source.IsPure_;

    if (source.Name_) {
        destination.Name_ = source.Name_;
    }

    for (const auto& resourceFile : source.ResourceFileList_) {
        destination.ResourceFileList_.push_back(resourceFile);
    }
}

bool TFnAttributesOps::GetIsPure(const TFnAttributes& attributes)
{
    return attributes.IsPure_;
}

const std::vector<TString> TFnAttributesOps::GetResourceFileList(const TFnAttributes& attributes)
{
    return attributes.ResourceFileList_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
