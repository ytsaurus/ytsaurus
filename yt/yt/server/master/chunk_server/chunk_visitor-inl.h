#ifndef CHUNK_VISITOR_INL_H
#error "Direct inclusion of this file is not allowed, include chunk_visitor.h"
// For the sake of sane code completion.
#include "chunk_visitor.h"
#endif

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/misc/optional.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TKeyExtractor, class TKeyFormatter>
class TChunkStatisticsVisitor
    : public TChunkVisitorBase
{
public:
    TChunkStatisticsVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkLists chunkLists,
        TKeyExtractor keyExtractor,
        TKeyFormatter keyFormatter)
        : TChunkVisitorBase(bootstrap, chunkLists)
        , KeyExtractor_(std::move(keyExtractor))
        , KeyFormatter_(std::move(keyFormatter))
    { }

private:
    const TKeyExtractor KeyExtractor_;
    const TKeyFormatter KeyFormatter_;

    using TOptionalTraits = TStdOptionalTraits<
        typename std::invoke_result_t<TKeyExtractor, const TChunk*>
    >;
    using TKey = typename TOptionalTraits::TValueType;
    static constexpr bool IsOptional = TOptionalTraits::IsStdOptional;

    struct TStatistics
    {
        TChunkTreeStatistics ChunkTreeStatistics;
        i64 MaxBlockSize = 0;
    };

    using TStatisticsMap = THashMap<TKey, TStatistics>;
    TStatisticsMap StatisticsMap_;

    bool OnChunk(
        TChunk* chunk,
        TChunkList* /*parent*/,
        std::optional<i64> /*rowIndex*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/,
        const TChunkViewModifier* /*modifier*/) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto maybeKey = KeyExtractor_(chunk);
        TKey key;

        if constexpr (IsOptional) {
            if (!maybeKey) {
                return true;
            }
            key = std::move(*maybeKey);
        } else {
            key = std::move(maybeKey);
        }

        auto& statistics = StatisticsMap_[key];
        statistics.ChunkTreeStatistics.Accumulate(chunk->GetStatistics());
        statistics.MaxBlockSize = std::max(statistics.MaxBlockSize, chunk->GetMaxBlockSize());

        return true;
    }

    bool OnChunkView(TChunkView* /*chunkView*/) override
    {
        return false;
    }

    bool OnDynamicStore(
        TDynamicStore* /*dynamicStore*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/) override
    {
        return true;
    }

    void OnSuccess() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto result = NYTree::BuildYsonStringFluently()
            .DoMapFor(StatisticsMap_, [this] (NYTree::TFluentMap fluent, const typename TStatisticsMap::value_type& pair) {
                const auto& statistics = pair.second;
                // TODO(panin): maybe use here the same method as in attributes
                fluent
                    .Item(KeyFormatter_(pair.first)).BeginMap()
                        .Item("chunk_count").Value(statistics.ChunkTreeStatistics.ChunkCount)
                        .Item("uncompressed_data_size").Value(statistics.ChunkTreeStatistics.UncompressedDataSize)
                        .Item("compressed_data_size").Value(statistics.ChunkTreeStatistics.CompressedDataSize)
                        .Item("data_weight").Value(statistics.ChunkTreeStatistics.DataWeight)
                        .Item("max_block_size").Value(statistics.MaxBlockSize)
                    .EndMap();
            });
        Promise_.Set(result);
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TDefaultKeyFormatter
{
    template <class E>
    TString operator()(E value) const
        requires TEnumTraits<E>::IsEnum
    {
        return FormatEnum(value);
    }

    TString operator()(NObjectClient::TCellTag value) const
    {
        return ToString(value);
    }
};

} // namespace NDetail

template <class TKeyExtractor>
TFuture<NYson::TYsonString> ComputeChunkStatistics(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkLists& chunkLists,
    TKeyExtractor keyExtractor)
{
    return ComputeChunkStatistics(
        bootstrap,
        chunkLists,
        std::move(keyExtractor),
        NDetail::TDefaultKeyFormatter{});
}

template <class TKeyExtractor, class TKeyFormatter>
TFuture<NYson::TYsonString> ComputeChunkStatistics(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkLists& chunkLists,
    TKeyExtractor keyExtractor,
    TKeyFormatter keyFormatter)
{
    auto visitor = New<TChunkStatisticsVisitor<TKeyExtractor, TKeyFormatter>>(
        bootstrap,
        chunkLists,
        std::move(keyExtractor),
        std::move(keyFormatter));
    return visitor->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

