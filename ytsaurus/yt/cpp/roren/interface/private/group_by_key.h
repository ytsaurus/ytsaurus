#pragma once

#include "raw_transform.h"
#include "raw_data_flow.h"

#include "../input.h"
#include "../key_value.h"
#include "../type_tag.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename K, typename V>
IRawGroupByKeyPtr MakeRawGroupByKey();

////////////////////////////////////////////////////////////////////////////////

template <typename K, typename V>
class TValueIterator
    : public IRawInput
{
public:
    explicit TValueIterator(const V* first, TInputPtr<TKV<K, V>> inner)
        : First_(first)
        , Inner_(std::move(inner))
    { }

    const void* NextRaw() override
    {
        if (First_) {
            auto result = First_;
            First_ = nullptr;
            return result;
        }
        const auto* kv = Inner_->Next();
        if (kv == nullptr) {
            return nullptr;
        }
        return &kv->Value();
    }

private:
    const V* First_;
    TInputPtr<TKV<K, V>> Inner_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename K, typename V>
class TRawGroupByKey
    : public IRawGroupByKey
{
public:
    void ProcessOneGroup(const IRawInputPtr& rawInput, const IRawOutputPtr& rawOutput) override
    {
        auto input = TInputPtr<TKV<K, V>>{rawInput};
        auto upcastedOutput = rawOutput->Upcast<TKV<K, TInputPtr<V>>>();

        const auto* first = input->Next();
        Y_VERIFY(first);
        TKV<K, TInputPtr<V>> result;
        result.Key() = first->Key();
        result.Value() = TInputPtr<V>{MakeIntrusive<TValueIterator<K, V>>(&first->Value(), std::move(input))};
        upcastedOutput->Add(result);
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {InputTag_};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {OutputTag_};
    }

private:
    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawGroupByKeyPtr {
            return ::MakeIntrusive<TRawGroupByKey<K, V>>();
        };
    }

    void SaveState(IOutputStream&) const override
    { }

    void LoadState(IInputStream&) override
    { }

private:
    const TDynamicTypeTag InputTag_ = TTypeTag<TKV<K, V>>("group-by-key-input-0");
    const TDynamicTypeTag OutputTag_ = TTypeTag<TKV<K, TInputPtr<V>>>("group-by-key-output-0");
};

////////////////////////////////////////////////////////////////////////////////

template <typename K, typename V>
IRawGroupByKeyPtr MakeRawGroupByKey()
{
    return ::MakeIntrusive<TRawGroupByKey<K, V>>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
