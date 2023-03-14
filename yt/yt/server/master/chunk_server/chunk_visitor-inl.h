#ifndef CHUNK_VISITOR_INL_H
#error "Direct inclusion of this file is not allowed, include chunk_visitor.h"
// For the sake of sane code completion.
#include "chunk_visitor.h"
#endif

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TKeyExtractor, class TPredicate>
class TChunkStatisticsVisitor
    : public TChunkVisitorBase
{
public:
    TChunkStatisticsVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkLists chunkLists,
        TKeyExtractor keyExtractor,
        TPredicate predicate)
        : TChunkVisitorBase(bootstrap, chunkLists)
        , KeyExtractor_(std::move(keyExtractor))
        , Predicate_(std::move(predicate))
    { }

private:
    const TKeyExtractor KeyExtractor_;
    const TPredicate Predicate_;

    struct TStatistics
    {
        TChunkTreeStatistics ChunkTreeStatistics;
        i64 MaxBlockSize = 0;
    };

    using TKey = typename std::invoke_result_t<TKeyExtractor, const TChunk*>;
    using TStatiticsMap = THashMap<TKey, TStatistics>;
    TStatiticsMap StatisticsMap_;

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

        if (Predicate_(chunk)) {
            auto& statistics = StatisticsMap_[KeyExtractor_(chunk)];
            statistics.ChunkTreeStatistics.Accumulate(chunk->GetStatistics());
            statistics.MaxBlockSize = std::max(statistics.MaxBlockSize, chunk->GetMaxBlockSize());
        }
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
            .DoMapFor(StatisticsMap_, [=] (NYTree::TFluentMap fluent, const typename TStatiticsMap::value_type& pair) {
                const auto& statistics = pair.second;
                // TODO(panin): maybe use here the same method as in attributes
                fluent
                    .Item(FormatKey(pair.first)).BeginMap()
                        .Item("chunk_count").Value(statistics.ChunkTreeStatistics.ChunkCount)
                        .Item("uncompressed_data_size").Value(statistics.ChunkTreeStatistics.UncompressedDataSize)
                        .Item("compressed_data_size").Value(statistics.ChunkTreeStatistics.CompressedDataSize)
                        .Item("data_weight").Value(statistics.ChunkTreeStatistics.DataWeight)
                        .Item("max_block_size").Value(statistics.MaxBlockSize)
                    .EndMap();
            });
        Promise_.Set(result);
    }

    template <class T>
        requires TEnumTraits<T>::IsEnum
    static TString FormatKey(T value)
    {
        return FormatEnum(value);
    }

    static TString FormatKey(NObjectClient::TCellTag value)
    {
        return ToString(value);
    }
};

////////////////////////////////////////////////////////////////////////////////

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
        [] (const TChunk* /*chunk*/) { return true; });
}

template <class TKeyExtractor, class TPredicate>
TFuture<NYson::TYsonString> ComputeChunkStatistics(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkLists& chunkLists,
    TKeyExtractor keyExtractor,
    TPredicate predicate)
{
    auto visitor = New<TChunkStatisticsVisitor<TKeyExtractor, TPredicate>>(
        bootstrap,
        chunkLists,
        std::move(keyExtractor),
        std::move(predicate));
    return visitor->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

