#include "fn_attributes_ops.h"
#include <yt/cpp/roren/interface/fns.h>


namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void TFnAttributesOps::Merge(TFnAttributes& destination, const TFnAttributes& source)
{
    destination.IsPure_ = destination.IsPure_ && source.IsPure_;
    for (const auto& resourceFile : source.ResourceFileList_) {
        destination.ResourceFileList_.push_back(resourceFile);
    }
}

bool TFnAttributesOps::GetIsPure(const TFnAttributes& attributes)
{
    return attributes.IsPure_;
}

void TFnAttributesOps::SetIsMove(TFnAttributes& attributes, bool isMove)
{
    attributes.IsMove_ = isMove;
}

bool TFnAttributesOps::GetIsMove(const TFnAttributes& attributes)
{
    return attributes.IsMove_;
}

const std::vector<TString> TFnAttributesOps::GetResourceFileList(const TFnAttributes& attributes)
{
    return attributes.ResourceFileList_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
