#pragma once

#include "fwd.h"

#include "input.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {
inline TCoGbkResult MakeCoGbkResult(NPrivate::TRawRowHolder key, std::vector<std::pair<TDynamicTypeTag, NPrivate::IRawInputPtr>> taggedInputs);
inline void SetKey(TCoGbkResult& gbk, const void* key);
}

////////////////////////////////////////////////////////////////////////////////

//
// Result of CoGroupByKey operation
//
// Contains a key and the number of TInputPtr's that iterate over TKV's with the same key.
class TCoGbkResult
{
public:
    TCoGbkResult() = default;
    TCoGbkResult(const TCoGbkResult& other) = default;
    TCoGbkResult(TCoGbkResult&& other) = default;

    // Get the key
    //
    // It's user responsibility to specify proper type.
    template <CRow TKey>
    const TKey& GetKey() const
    {
        Y_ASSERT(typeid(TKey).name() == RawKey_.GetRowVtable().TypeName);
        return *static_cast<const TKey*>(RawKey_.GetData());
    }

    // Get input with specified tag
    template <CRow TRow>
    TInput<TRow>& GetInput(TTypeTag<TRow> tag) const
    {
        return MultiInput_->GetInput(tag);
    }

    std::vector<TString> GetInputDescriptionList() const
    {
        return MultiInput_->GetInputDescriptionList();
    }

private:
    TCoGbkResult(NPrivate::TRawRowHolder key, const std::vector<std::pair<TDynamicTypeTag, NPrivate::IRawInputPtr>>& taggedInputs)
        : RawKey_(std::move(key))
        , MultiInput_(CreateMultiInput(taggedInputs))
    { }

private:
    NPrivate::TRawRowHolder RawKey_;
    TMultiInputPtr MultiInput_;

private:
    friend class TCoder<TCoGbkResult>;
    friend TCoGbkResult NPrivate::MakeCoGbkResult(NPrivate::TRawRowHolder key, std::vector<std::pair<TDynamicTypeTag, NPrivate::IRawInputPtr>> taggedInputs);
    friend void NPrivate::SetKey(TCoGbkResult& gbk, const void* key);
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TCoder<TCoGbkResult>
{
public:
    void Encode(IOutputStream* out, const TCoGbkResult& gbk)
    {
        KeyCoder_.Encode(out, gbk.RawKey_);
        MultiInputCoder_.Encode(out, gbk.MultiInput_);
    }

    void Decode(IInputStream* in, TCoGbkResult& gbk)
    {
        KeyCoder_.Decode(in, gbk.RawKey_);
        MultiInputCoder_.Decode(in, gbk.MultiInput_);
    }

private:
    TCoder<NPrivate::TRawRowHolder> KeyCoder_;
    TCoder<TMultiInputPtr> MultiInputCoder_;
};

////////////////////////////////////////////////////////////////////////////////

inline TCoGbkResult NPrivate::MakeCoGbkResult(NPrivate::TRawRowHolder key, std::vector<std::pair<TDynamicTypeTag, NPrivate::IRawInputPtr>> taggedInputs)
{
    return TCoGbkResult{std::move(key), std::move(taggedInputs)};
}

inline void NPrivate::SetKey(TCoGbkResult& gbk, const void* key)
{
    gbk.RawKey_.CopyFrom(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
