#pragma once

#include "fwd.h"

#include "row.h"
#include "type_tag.h"

#include "private/raw_data_flow.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename TRowType_>
class TOutput
    : private NPrivate::IRawOutput
{
public:
    using TRowType = TRowType_;

public:
    TOutput() = delete;

    Y_FORCE_INLINE void Add(const TRowType& row)
    {
        static_assert(sizeof(*this) == sizeof(NPrivate::IRawOutput));

        AddRaw(&row, 1);
    }

    // NB. This class is thin typed wrapper around IRawOutput it allows compiler to type check program.
    // This class MUST NOT contain any data members.
};

template <>
class TOutput<void>
{
public:
    using TRowType = void;
};

extern TOutput<void> VoidOutput;

template <typename T>
concept CRowOutput = TIsSpecializationOf<TOutput, T>::value && !std::is_same_v<T, TOutput<TMultiRow>>;

template <>
class TOutput<TMultiRow>
{
public:
    TOutput(const std::vector<TDynamicTypeTag>& tags, const std::vector<NPrivate::IRawOutputPtr>& outputs)
    {
        Y_VERIFY(tags.size() == outputs.size());
        Y_VERIFY(!tags.empty());
        for (ssize_t i = 0; i < std::ssize(tags); ++i) {
            const auto& tag = tags[i];
            TKey key = tag.GetKey();
            auto emplaceResult = OutputMap_.emplace(key, outputs[i]);
            Y_VERIFY(emplaceResult.second, "tags are not unique, duplicating tag: %s", ToString(tag).c_str());
        }
    }

    template <CRow TRow>
    TOutput<TRow>& GetOutput(TTypeTag<TRow> tag)
    {
        TKey key = TDynamicTypeTag(tag).GetKey();
        auto it = OutputMap_.find(key);
        if (it == OutputMap_.end()) {
            Y_FAIL("Unknown tag: %s", tag.GetDescription().c_str());
        }
        return *it->second->template Upcast<TRow>();
    }

private:
    using TKey = TDynamicTypeTag::TKey;

    THashMap<TKey, NPrivate::IRawOutputPtr> OutputMap_;
};

template <typename T>
concept CMultiOutput = std::is_same_v<TMultiOutput, T>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
