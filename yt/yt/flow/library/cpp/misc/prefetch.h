#pragma once

#include <util/system/compiler.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <tuple>
#include <utility>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// Software-pipelined prefetch over a random-access container.
//
// A prefetcher is a list of stages built with Add(). At position |i| a stage is applied to the
// element some offset ahead (items[i + offset]). ForEach walks the container, runs every stage
// ahead of the body, and — via a prologue that prefetches the leading elements too — makes sure no
// element is processed cold.
//
// Offsets are given as deltas: Add(delta, fn) places a stage |delta| further ahead than the stage
// added after it (the closer one); the last-added stage sits |delta| ahead of the body. Add(fn)
// uses the default stride. So .Add(a).Add(b).Add(c) at the default stride 16 places a at 48, b at
// 32, c at 16 — the natural staged order for multi-level prefetch, where an outer object must be
// warm before an inner stage can read a pointer out of it. Deltas are runtime values so a
// microbenchmark can sweep them (see benchmarks/prefetch_bench.cpp).
//
// |items| is taken by (deduced) reference, so the body sees mutable elements for a non-const
// container (e.g. to std::move them out) and const elements otherwise.
//
// Usage (two staged levels):
//   MakePrefetcher()
//       .Add([] (const TItem& item) { Y_PREFETCH_READ(item.Get(), 3); })   // object, offset 32
//       .Add([] (const TItem& item) { item->Nested.Prefetch(); })          // nested, offset 16
//       .ForEach(items, [&] (const TItem& item) { /* body */ });

inline constexpr size_t DefaultPrefetchStride = 16;

template <class TFn>
struct TPrefetchStage
{
    size_t Delta;
    TFn Fn;
};

template <class... TStages>
class TPrefetcher
{
public:
    explicit TPrefetcher(std::tuple<TStages...> stages)
        : Stages_(std::move(stages))
    { }

    // Adds a stage |delta| further ahead than the previously added (closer) stage.
    template <class TFn>
    TPrefetcher<TStages..., TPrefetchStage<TFn>> Add(size_t delta, TFn fn) const
    {
        return TPrefetcher<TStages..., TPrefetchStage<TFn>>(
            std::tuple_cat(Stages_, std::make_tuple(TPrefetchStage<TFn>{delta, std::move(fn)})));
    }

    // Adds a stage the default stride further ahead than the previous one.
    template <class TFn>
    TPrefetcher<TStages..., TPrefetchStage<TFn>> Add(TFn fn) const
    {
        return Add(DefaultPrefetchStride, std::move(fn));
    }

    template <class TItems, class TBody>
    void ForEach(TItems& items, TBody body) const
    {
        constexpr auto stages = std::make_index_sequence<sizeof...(TStages)>{};
        const auto offsets = ResolveOffsets(stages);
        size_t maxOffset = 0;
        for (size_t offset : offsets) {
            maxOffset = std::max(maxOffset, offset);
        }

        const size_t size = items.size();

        // Warm the leading elements that no main-loop iteration prefetches ahead.
        ForStages(stages, offsets, [&] (const auto& stage, size_t offset) {
            for (size_t j = 0, head = std::min(offset, size); j < head; ++j) {
                stage.Fn(items[j]);
            }
        });

        // Middle: every stage target (i + offset <= i + maxOffset) is in range, so no bounds checks.
        const size_t middle = size > maxOffset ? size - maxOffset : 0;
        for (size_t i = 0; i < middle; ++i) {
            ForStages(stages, offsets, [&] (const auto& stage, size_t offset) {
                stage.Fn(items[i + offset]);
            });
            body(items[i]);
        }
        // Tail: the last maxOffset elements, where some stage targets fall past the end.
        for (size_t i = middle; i < size; ++i) {
            ForStages(stages, offsets, [&] (const auto& stage, size_t offset) {
                if (i + offset < size) {
                    stage.Fn(items[i + offset]);
                }
            });
            body(items[i]);
        }
    }

private:
    const std::tuple<TStages...> Stages_;

    // Turns per-stage deltas into absolute offsets: the last-added (closest) stage sits at its own
    // delta from the body, each earlier stage that much further than the one after it.
    template <size_t... Is>
    std::array<size_t, sizeof...(Is)> ResolveOffsets(std::index_sequence<Is...>) const
    {
        constexpr size_t stageCount = sizeof...(Is);
        const std::array<size_t, stageCount> deltas = {{std::get<Is>(Stages_).Delta...}};
        std::array<size_t, stageCount> offsets{};
        size_t accumulated = 0;
        for (size_t k = stageCount; k-- > 0;) {
            accumulated += deltas[k];
            offsets[k] = accumulated;
        }
        return offsets;
    }

    template <size_t... Is, class TFn>
    Y_FORCE_INLINE void ForStages(std::index_sequence<Is...>, const std::array<size_t, sizeof...(Is)>& offsets, TFn&& fn) const
    {
        (fn(std::get<Is>(Stages_), offsets[Is]), ...);
    }
};

inline TPrefetcher<> MakePrefetcher()
{
    return TPrefetcher<>(std::tuple<>{});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
