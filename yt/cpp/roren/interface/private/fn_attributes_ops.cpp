#include "fn_attributes_ops.h"
#include <yt/cpp/roren/interface/fns.h>


namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void TFnAttributesOps::Merge(TFnAttributes& destination, const TFnAttributes& source, bool isPatch)
{
    if (isPatch) {
        if (source.IsPure_.has_value()) {
            destination.IsPure_ = source.IsPure_;
        }
    } else {
        destination.IsPure_ = TFnAttributesOps::GetIsPure(destination) && TFnAttributesOps::GetIsPure(source);
    }
    for (const auto& resourceFile : source.ResourceFileList_) {
        destination.ResourceFileList_.push_back(resourceFile);
    }
}

void TFnAttributesOps::Merge(TFnAttributes& destination, const TFnAttributes& source)
{
    Merge(destination, source, false);
}

void TFnAttributesOps::MergePatch(TFnAttributes& destination, const TFnAttributes& source)
{
    Merge(destination, source, true);
}

bool TFnAttributesOps::GetIsPure(const TFnAttributes& attributes)
{
    return attributes.IsPure_.value_or(false);
}

const std::vector<TString> TFnAttributesOps::GetResourceFileList(const TFnAttributes& attributes)
{
    return attributes.ResourceFileList_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
