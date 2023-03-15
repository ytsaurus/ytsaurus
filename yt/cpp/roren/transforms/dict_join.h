#pragma once

#include <yt/cpp/roren/interface/roren.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

//! Transforms that joins other PCollection with dictionary.
///
/// DictJoin transform expects input of type
///   TKV<TKey, TMainValue>
/// where:
///   - TKey must match type of dictionary key;
///   - TMainValue is arbitrary type (allowed to be used in PCollections)
///
/// DictJoin transform returns PCollection of type
///   std::tuple<TKey, TMainValue, std::optional<TDictValue>>
template <typename TKey, typename TDictValue>
auto DictJoin(TPCollection<TKV<TKey, TDictValue>> dict);

////////////////////////////////////////////////////////////////////////////////

template <typename K, typename V>
class IDictResolver
    : public NPrivate::ISerializable<IDictResolver<K, V>>
{
public:
    using TKey = K;
    using TValue = V;

public:
    virtual void Start(const IExecutionContextPtr& /*executionContext*/)
    { }

    virtual std::vector<std::optional<V>> Resolve(std::span<const K> keys, const IExecutionContextPtr& executionContext) = 0;

    virtual void Finish(const IExecutionContextPtr& /*executionContext*/)
    { }
};

template <typename K, typename V>
using IDictResolverPtr = ::TIntrusivePtr<IDictResolver<K, V>>;

////////////////////////////////////////////////////////////////////////////////

struct TResolveKeysOptions
{
    size_t BatchSize = 0;

    Y_SAVELOAD_DEFINE(BatchSize);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TKey, typename TPrimaryValue, typename TDictValue>
class TResolveKeysParDo
    : public IBatchDoFn<TKV<TKey, TPrimaryValue>, std::tuple<TKey, TPrimaryValue, std::optional<TDictValue>>>
{
public:
    TResolveKeysParDo() = default;

    TResolveKeysParDo(IDictResolverPtr<TKey, TDictValue> dict, TResolveKeysOptions options)
        : Resolver_(std::move(dict))
        , Options_(std::move(options))
    { }

    void Start(TOutput<std::tuple<TKey, TPrimaryValue, std::optional<TDictValue>>>&) override
    {
        Batch_.clear();
        Resolver_->Start(TBase::GetExecutionContext());
    }

    void Do(std::span<const TKV<TKey, TPrimaryValue>> input, TOutput<std::tuple<TKey, TPrimaryValue, std::optional<TDictValue>>>& output) override
    {
        Batch_.insert(Batch_.end(), input.begin(), input.end());
        Flush(output, false);
    }

    void Finish(TOutput<std::tuple<TKey, TPrimaryValue, std::optional<TDictValue>>>& output) override
    {
        Flush(output, true);
        Resolver_->Finish(TBase::GetExecutionContext());
    }

    void Save(IOutputStream* out) const override
    {
        NPrivate::SaveSerializable(out, Resolver_);
        ::Save(out, Options_);
    }

    void Load(IInputStream* in) override
    {
        NPrivate::LoadSerializable(in, Resolver_);
        ::Load(in, Options_);
    }

private:
    void Flush(TOutput<std::tuple<TKey, TPrimaryValue, std::optional<TDictValue>>>& output, bool force) {
        if (!force && Batch_.size() < Options_.BatchSize) {
            return;
        }

        std::vector<TKey> keys;
        keys.reserve(Batch_.size());
        for (const auto& v : Batch_) {
            keys.push_back(v.Key());
        }

        auto resolved = Resolver_->Resolve(keys, TBase::GetExecutionContext());

        Y_VERIFY(resolved.size() == Batch_.size());

        for (ssize_t i = 0; i < std::ssize(resolved); ++i) {
            std::tuple<TKey, TPrimaryValue, std::optional<TDictValue>> t = {Batch_[i].Key(), Batch_[i].Value(), resolved[i]};
            output.Add(t);
        }

        Batch_.clear();
    }

private:
    using TBase = IBatchDoFn<TKV<TKey, TPrimaryValue>, std::tuple<TKey, TPrimaryValue, std::optional<TDictValue>>>;

private:
    IDictResolverPtr<TKey,TDictValue> Resolver_;
    TResolveKeysOptions Options_;

    std::vector<TKV<TKey, TPrimaryValue>> Batch_;
};

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename K, typename V>
TTypeTag<IDictResolverPtr<K, V>> DictResolverTag()
{
    return TTypeTag<IDictResolverPtr<K, V>>{"dict_resolver"};
}

extern const TTypeTag<TResolveKeysOptions> ResolveKeysOptionsTag;

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename TKey, typename TDictValue>
class TDictJoinTransform
{
public:
    template <typename TPrimaryValue>
    using TResult = std::tuple<TKey, TPrimaryValue, std::optional<TDictValue>>;

public:
    TDictJoinTransform(TPCollection<TKV<TKey, TDictValue>> pCollection)
        : DictPCollection_(std::move(pCollection))
    { }

    template <typename TPrimaryValue>
    TPCollection<TResult<TPrimaryValue>> ApplyTo(const TPCollection<TKV<TKey, TPrimaryValue>>& primaryPCollection) const
    {
        auto rawNode = NPrivate::GetRawDataNode(DictPCollection_);
        if (auto dictResolverPtrPtr = NPrivate::GetAttribute(*rawNode, NPrivate::DictResolverTag<TKey, TDictValue>())) {
            auto options = NPrivate::GetAttributeOrDefault(*rawNode, NPrivate::ResolveKeysOptionsTag, {});
            auto resolveKeysParDo = MakeParDo<TResolveKeysParDo<TKey, TPrimaryValue, TDictValue>>(*dictResolverPtrPtr, options);
            return primaryPCollection | resolveKeysParDo;
        } else {
            auto pipeline = NPrivate::GetRawPipeline(primaryPCollection);

            static const auto dictTag = TTypeTag<TKV<TKey, TDictValue>>("dict");
            static const auto primaryTag = TTypeTag<TKV<TKey, TPrimaryValue>>("primary");

            auto multiPCollection = TMultiPCollection{dictTag, DictPCollection_, primaryTag, primaryPCollection};

            return multiPCollection | CoGroupByKey() | ParDo(JoinFunc<TPrimaryValue>);
        }
    }

private:
    template <typename TPrimaryValue>
    static void JoinFunc(const TCoGbkResult& gbk, TOutput<std::tuple<TKey, TPrimaryValue, std::optional<TDictValue>>>& output)
    {
        static const auto dictTag = TTypeTag<TKV<TKey, TDictValue>>("dict");
        static const auto primaryTag = TTypeTag<TKV<TKey, TPrimaryValue>>("primary");

        std::tuple<TKey, TPrimaryValue, std::optional<TDictValue>> result;
        auto& [key, primaryValue, dictValue] = result;
        auto dictKV = ReadOptionalRow(gbk.GetInput(dictTag));
        if (dictKV) {
            dictValue = dictKV->Value();
        }

        key = gbk.GetKey<TKey>();

        for (const auto& v : gbk.GetInput(primaryTag)) {
            primaryValue = v.Value();
            output.Add(result);
        }
    }

private:
    TPCollection<TKV<TKey, TDictValue>> DictPCollection_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TKey, typename TDictValue>
auto DictJoin(TPCollection<TKV<TKey, TDictValue>> dict)
{
    return TDictJoinTransform{std::move(dict)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
