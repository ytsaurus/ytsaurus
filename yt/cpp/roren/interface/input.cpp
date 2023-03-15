#include "input.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

TMultiInputPtr CreateMultiInput(const std::vector<std::pair<TDynamicTypeTag, NPrivate::IRawInputPtr>>& taggedInputs)
{
    auto result = ::MakeIntrusive<TMultiInput>();

    for (const auto& [tag, input] : taggedInputs) {
        auto key = tag.GetKey();
        auto emplaceResult = result->InputMap_.emplace(key, input);
        Y_VERIFY(emplaceResult.second, "tags are not unique, duplicating tag: %s", ToString(tag).c_str());
        result->Tags_.emplace_back(tag);
    }

    return result;
}

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <>
const void* TRawVectorIterator<NPrivate::TRawRowHolder>::NextRaw() {
    if (Iterator_ == Rows_.end()) {
        return nullptr;
    } else {
        return (Iterator_++)->GetData();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
