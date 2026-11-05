#include "shuffling_reader.h"

#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unordered_schemaful_reader.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ShuffleRowsByPrefix(
    const ISchemafulUnversionedReaderPtr& reader,
    TRange<ISchemafulPipePtr> pipes,
    int keyPrefixLength)
{
    int destinationCount = pipes.size();
    std::vector<std::vector<TUnversionedRow>> shuffledRows(destinationCount);
    std::vector<TFuture<void>> writerReadyEvents;

    while (auto batch = reader->Read()) {
        if (batch->IsEmpty()) {
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();

            continue;
        }

        auto rows = batch->MaterializeRows();
        for (auto row : rows) {
            auto hash = GetFarmFingerprint(row.FirstNElements(keyPrefixLength));
            auto destination = hash % destinationCount;
            shuffledRows[destination].push_back(row);
        }

        for (int destination = 0; destination < destinationCount; ++destination) {
            if (shuffledRows[destination].empty()) {
                continue;
            }
            auto writer = pipes[destination]->GetWriter();
            if (!writer->Write(shuffledRows[destination])) {
                writerReadyEvents.push_back(writer->GetReadyEvent());
            }
            shuffledRows[destination].clear();
        }

        // Currently useless due to lack of backpressure in pipes at the moment.
        WaitFor(AllSucceeded(std::exchange(writerReadyEvents, {})))
            .ThrowOnError();
    }

    auto writerClosedEvents = std::move(writerReadyEvents);
    writerClosedEvents.clear();
    for (const auto& pipe : pipes) {
        writerClosedEvents.push_back(pipe->GetWriter()->Close());
    }
    WaitForFast(AllSucceeded(std::move(writerClosedEvents)))
        .ThrowOnError();
}

std::vector<std::vector<ISchemafulPipePtr>> MakePipes(
    int destinationCount,
    int readerCount,
    const IMemoryChunkProviderPtr& memoryChunkProvider)
{
    std::vector<std::vector<ISchemafulPipePtr>> pipes(
        destinationCount,
        std::vector<ISchemafulPipePtr>(readerCount));

    constexpr int MaxFlushBatchCount = 4;

    for (int destination = 0; destination < destinationCount; ++destination) {
        for (int index = 0; index < readerCount; ++index) {
            pipes[destination][index] = CreateSchemafulPipe(memoryChunkProvider, MaxFlushBatchCount);
        }
    }

    return pipes;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::pair<std::vector<ISchemafulUnversionedReaderPtr>, std::vector<TFuture<void>>> ShuffleByPrefixHash(
    TRange<ISchemafulUnversionedReaderPtr> readers,
    int keyPrefix,
    int destinationCount,
    const IInvokerPtr& invoker,
    const IMemoryChunkProviderPtr& memoryChunkProvider)
{
    auto pipes = MakePipes(destinationCount, readers.size(), memoryChunkProvider);

    std::vector<ISchemafulUnversionedReaderPtr> shuffledReaders(destinationCount);

    std::vector<TFuture<void>> shuffleFutures(readers.size());

    for (int index = 0; index < std::ssize(readers); ++index) {
        std::vector<ISchemafulPipePtr> destinationPipes(destinationCount);
        for (int destination = 0; destination < destinationCount; ++destination) {
            destinationPipes[destination] = pipes[destination][index];
        }
        shuffleFutures[index] = BIND(ShuffleRowsByPrefix, readers[index], std::move(destinationPipes), keyPrefix)
            .AsyncVia(invoker)
            .Run()
            .Apply(BIND([pipes] (const TError& error) {
                if (!error.IsOK()) {
                    for (const auto& pipesForDestination : pipes) {
                        for (const auto& pipe : pipesForDestination) {
                            pipe->Fail(error);
                        }
                    }
                }
            }));
    }

    for (int destination = 0; destination < destinationCount; ++destination) {
        auto getNextReader = [pipes = std::move(pipes[destination]), index = 0] () mutable {
            if (index >= std::ssize(pipes)) {
                return ISchemafulUnversionedReaderPtr();
            }
            return pipes[index++]->GetReader();
        };
        shuffledReaders[destination] = CreateUnorderedSchemafulReader(getNextReader, readers.size());
    }

    return {shuffledReaders, std::move(shuffleFutures)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
