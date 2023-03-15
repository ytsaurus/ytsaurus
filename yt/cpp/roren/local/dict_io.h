#pragma once

#include "vector_io.h"

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/transforms/dict_join.h>

#include <util/generic/hash.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename TKey, typename TValue>
auto DictRead(THashMap<TKey, TValue> dict);

template <typename TKey, typename TValue>
class TDictReadTransform
{
private:
    class TResolver;

public:
    TDictReadTransform(THashMap<TKey, TValue> dict)
        : Resolver_(::MakeIntrusive<TResolver>(std::move(dict)))
    { }

    TPCollection<TKV<TKey, TValue>> ApplyTo(const TPipeline& pipeline) const
    {
        std::vector<TKV<TKey, TValue>> data;
        for (const auto& [key, value] : Resolver_->GetDict()) {
            data.emplace_back(key, value);
        }

        auto result = pipeline | VectorRead(std::move(data));
        auto resolver = static_cast<IDictResolverPtr<TKey, TValue>>(Resolver_);
        NPrivate::SetAttribute(*NPrivate::GetRawDataNode(result), NPrivate::DictResolverTag<TKey, TValue>(), resolver);
        return result;
    }

private:
    class TResolver
        : public IDictResolver<TKey, TValue>
    {
        public:
            TResolver() = default;

            TResolver(THashMap<TKey, TValue> dict)
                : Dict_(std::move(dict))
            { }

            const THashMap<TKey, TValue>& GetDict() const
            {
                return Dict_;
            }

            std::vector<std::optional<TValue>> Resolve(std::span<const TKey> keys, const IExecutionContextPtr&) override
            {
                std::vector<std::optional<TValue>> result;
                for (const auto& key : keys) {
                    if (auto it = Dict_.find(key); it != Dict_.end()) {
                        result.emplace_back(it->second);
                    } else {
                        result.emplace_back();
                    }
                }
                return result;
            }

            typename IDictResolver<TKey, TValue>::TDefaultFactoryFunc GetDefaultFactory() const override
            {
                return [] () -> IDictResolverPtr<TKey, TValue> {
                    return ::MakeIntrusive<TResolver>();
                };
            }

            void SaveState(IOutputStream& stream) const override
            {
                ::Save(&stream, Dict_);
            }

            void LoadState(IInputStream& stream) override
            {
                ::Load(&stream, Dict_);
            }

        private:
            THashMap<TKey, TValue> Dict_;
    };

private:
    ::TIntrusivePtr<TResolver> Resolver_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TKey, typename TValue>
auto DictRead(THashMap<TKey, TValue> dict)
{
    return TDictReadTransform(std::move(dict));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
