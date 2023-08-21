#include "co_group_by_key.h"

#include "raw_transform.h"

#include "../co_gbk_result.h"
#include "../type_tag.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TRawCoGroupByKey
    : public IRawCoGroupByKey
{
public:
    explicit TRawCoGroupByKey(std::vector<TDynamicTypeTag> inputTags)
        : InputTags_(std::move(inputTags))
    { }

    void SaveState(IOutputStream& output) const override
    {
        Save(&output, InputTags_);
    }

    void LoadState(IInputStream& input) override
    {
        Load(&input, InputTags_);
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return InputTags_;
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {
            {TTypeTag<TCoGbkResult>{"co-group-by-key-output-0"}},
        };
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawCoGroupByKeyPtr {
            return ::MakeIntrusive<TRawCoGroupByKey>(std::vector<TDynamicTypeTag>{});
        };
    }

private:
    std::vector<TDynamicTypeTag> InputTags_;

    IRawOutputPtr SingleOutput_;
};

IRawCoGroupByKeyPtr MakeRawCoGroupByKey(std::vector<TDynamicTypeTag> inputTags)
{
    return ::MakeIntrusive<TRawCoGroupByKey>(std::move(inputTags));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
