#include "composite_bigrt_writer.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

TCompositeBigRtWriter::TCompositeBigRtWriter(TWriterMap writerMap)
    : WriterMap_(std::move(writerMap))
{}

NBigRT::IWriterPtr TCompositeBigRtWriter::Clone()
{
    TWriterMap writerMap;
    writerMap.reserve(WriterMap_.size());

    for (const auto& [key, writer] : WriterMap_) {
        writerMap.emplace(key, writer->Clone());
    }

    return NYT::New<TCompositeBigRtWriter>(std::move(writerMap));
}

NYT::TFuture<NBigRT::IWriter::TTransactionWriter> TCompositeBigRtWriter::ExtractAsyncWriter()
{
    auto futures = TVector<NYT::TFuture<TTransactionWriter>>(Reserve(WriterMap_.size()));
    for (const auto& [key, writer] : WriterMap_) {
        futures.push_back(writer->ExtractAsyncWriter());
    }

    auto combinedFuture = NYT::AllSucceeded(std::move(futures));
    return combinedFuture.ApplyUnique(
        BIND([](std::vector<TTransactionWriter>&& transactionWriters) -> TTransactionWriter {
            return [transactionWriters = std::move(transactionWriters)](
                       NYT::NApi::ITransactionPtr tx, NYTEx::ITransactionContextPtr context) {
                for (auto& transactionWriter : transactionWriters) {
                    if (transactionWriter) {
                        transactionWriter(tx, context);
                    }
                }
            };
        }));
}

NBigRT::IWriterPtr TCompositeBigRtWriterFactory::Make(uint64_t shard, NSFStats::TSolomonContext solomonContext)
{
    TCompositeBigRtWriter::TWriterMap writerMap;
    writerMap.reserve(FactoryMap_.size());
    for (const auto& [key, factory] : FactoryMap_) {
        writerMap.emplace(key, factory->Make(shard, solomonContext));
    }
    return NYT::New<TCompositeBigRtWriter>(std::move(writerMap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
