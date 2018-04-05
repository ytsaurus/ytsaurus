#pragma once
#ifndef CHUNK_VISITOR_INL_H
#error "Direct inclusion of this file is not allowed, include chunk_visitor.h"
#endif

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TKeyExtractor>
class TChunkStatisticsVisitor
    : public TChunkVisitorBase
{
public:
    TChunkStatisticsVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        TKeyExtractor keyExtractor)
        : TChunkVisitorBase(bootstrap, chunkList)
        , KeyExtractor_(keyExtractor)
    { }

private:
    const TKeyExtractor KeyExtractor_;

    struct TStatistics
    {
        TChunkTreeStatistics ChunkTreeStatistics;
        i64 MaxBlockSize = 0;
    };

    using TKey = typename std::result_of<TKeyExtractor(const TChunk*)>::type;
    using TStatiticsMap = yhash<TKey, TStatistics>;
    TStatiticsMap StatisticsMap_;

    virtual bool OnChunk(
        TChunk* chunk,
        i64 /*rowIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto& statistics = StatisticsMap_[KeyExtractor_(chunk)];
        statistics.ChunkTreeStatistics.Accumulate(chunk->GetStatistics());
        statistics.MaxBlockSize = std::max(statistics.MaxBlockSize, chunk->MiscExt().max_block_size());
        return true;
    }

    virtual void OnSuccess() override
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
    static TString FormatKey(T value, typename TEnumTraits<T>::TType* = 0)
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
    TChunkList* chunkList,
    TKeyExtractor keyExtractor)
{
    auto visitor = New<TChunkStatisticsVisitor<TKeyExtractor>>(
        bootstrap,
        chunkList,
        keyExtractor);
    return visitor->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

