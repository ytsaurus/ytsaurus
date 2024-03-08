#include "dictionary_compression_session.h"
#include "chunk_meta_extensions.h"
#include "hunks.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/erasure/public.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NCompression;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDictionaryDecompressionSession)

class TDictionaryDecompressionSession
    : public IDictionaryDecompressionSession
{
public:
    TDictionaryDecompressionSession(
        TWeakPtr<IDictionaryCompressionFactory> dictionaryCompressionFactory,
        TDictionaryCompressionSessionConfigPtr config,
        NLogging::TLogger logger)
        : DictionaryCompressionFactory_(dictionaryCompressionFactory)
        , Config_(std::move(config))
        , Logger(std::move(logger))
    { }

    TFuture<std::vector<TSharedRef>> DecompressValues(
        std::vector<TUnversionedValue*> values,
        std::vector<TChunkId> dictionaryIds,
        TClientChunkReadOptions chunkReadOptions) override
    {
        YT_VERIFY(values.size() == dictionaryIds.size());

        YT_VERIFY(!Promise_);
        Promise_ = NewPromise<std::vector<TSharedRef>>();

        Logger.AddTag("ReadSessionId: %v",
            chunkReadOptions.ReadSessionId);

        auto dictionaryCompressionFactory = DictionaryCompressionFactory_.Lock();
        if (!dictionaryCompressionFactory) {
            Promise_.TrySet(TError(NYT::EErrorCode::Canceled,
                "Unable to decompress value due to dictionary compression factory being destroyed"));
            return Promise_;
        }

        THashSet<TChunkId> dictionaryIdSet(dictionaryIds.begin(), dictionaryIds.end());
        YT_VERIFY(!dictionaryIdSet.contains(NullChunkId));

        YT_LOG_DEBUG("Dictionary decompression session will fetch decompressors from cache (DictionaryIds: %v)",
            dictionaryIdSet);

        auto decompressorsFuture = dictionaryCompressionFactory->GetDecompressors(
            chunkReadOptions,
            dictionaryIdSet);

        if (auto maybeDecompressors = decompressorsFuture.TryGetUnique();
            maybeDecompressors && maybeDecompressors->IsOK())
        {
            DoDecompressValues(
                /*decompressedValueCount*/ 0,
                std::move(values),
                std::move(dictionaryIds),
                std::move(chunkReadOptions),
                TErrorOr<THashMap<TChunkId, TRowDictionaryDecompressor>>(std::move(*maybeDecompressors)));
        } else {
            decompressorsFuture.SubscribeUnique(BIND(
                &TDictionaryDecompressionSession::DoDecompressValues,
                MakeStrong(this),
                /*decompressedValueCount*/ 0,
                Passed(std::move(values)),
                Passed(std::move(dictionaryIds)),
                Passed(std::move(chunkReadOptions)))
                .Via(GetCurrentInvoker()));
        }

        return Promise_;
    }

    TDuration GetDecompressionTime() const override
    {
        return DecompressionTime_;
    }

private:
    struct TDecompressionSessionDataTag
    { };

    const TWeakPtr<IDictionaryCompressionFactory> DictionaryCompressionFactory_;
    const TDictionaryCompressionSessionConfigPtr Config_;

    NLogging::TLogger Logger;

    TPromise<std::vector<TSharedRef>> Promise_;
    std::vector<TSharedRef> Blobs_;

    TDuration DecompressionTime_;


    void DoDecompressValues(
        int decompressedValueCount,
        std::vector<TUnversionedValue*> values,
        std::vector<TChunkId> dictionaryIds,
        TClientChunkReadOptions chunkReadOptions,
        TErrorOr<THashMap<TChunkId, TRowDictionaryDecompressor>>&& decompressorsOrError)
    {
        // Shortcut.
        if (Promise_.IsSet()) {
            return;
        }

        if (!decompressorsOrError.IsOK()) {
            Promise_.TrySet(TError(decompressorsOrError));
            return;
        }

        const auto& decompressors = decompressorsOrError.Value();

        auto* codec = GetDictionaryCompressionCodec();

        TWallTimer decompressionTimer;

        i64 decompressedSize = 0;
        std::vector<i64> contentSizes;
        auto newDecompressedValueCount = decompressedValueCount;
        while (newDecompressedValueCount < std::ssize(values)) {
            auto dictionaryId = dictionaryIds[newDecompressedValueCount];
            YT_VERIFY(dictionaryId);

            auto* compressedValue = values[newDecompressedValueCount];
            YT_VERIFY(IsStringLikeType(compressedValue->Type));
            YT_VERIFY(None(compressedValue->Flags & EValueFlags::Hunk));

            ++newDecompressedValueCount;

            const auto& rowDecompressor = GetOrCrash(decompressors, dictionaryId);
            if (!rowDecompressor.contains(compressedValue->Id)) {
                contentSizes.push_back(-1);
                continue;
            }

            auto frameInfo = codec->GetFrameInfo(TRef(compressedValue->Data.String, compressedValue->Length));
            contentSizes.push_back(frameInfo.ContentSize);
            decompressedSize += contentSizes.back();

            if (decompressedSize >= Config_->MaxDecompressionBlobSize) {
                break;
            }
        }

        auto blob = TSharedMutableRef::Allocate<TDecompressionSessionDataTag>(
            decompressedSize,
            { .InitializeStorage = false });
        Blobs_.push_back(blob);

        i64 currentCompressedSize = 0;
        i64 currentDecompressedSize = 0;
        i64 currentUncompressedSize = 0;
        for (int valueIndex = decompressedValueCount; valueIndex < newDecompressedValueCount; ++valueIndex) {
            auto dictionaryId = dictionaryIds[valueIndex];
            YT_VERIFY(dictionaryId);

            auto* compressedValue = values[valueIndex];
            TRef compressedValueRef(compressedValue->Data.String, compressedValue->Length);

            const auto& rowDecompressor = GetOrCrash(decompressors, dictionaryId);
            auto columnDecompressorIt = rowDecompressor.find(compressedValue->Id);
            if (columnDecompressorIt == rowDecompressor.end()) {
                currentUncompressedSize += compressedValue->Length;
                continue;
            }

            TMutableRef output(
                blob.Begin() + currentDecompressedSize,
                contentSizes[valueIndex - decompressedValueCount]);
            columnDecompressorIt->second->Decompress(compressedValueRef, output);

            currentCompressedSize += compressedValue->Length;
            currentDecompressedSize += output.Size();

            compressedValue->Data.String = output.Begin();
            compressedValue->Length = output.Size();
        }

        YT_VERIFY(currentDecompressedSize == std::ssize(blob));

        YT_LOG_DEBUG("Dictionary decompression session successfully decompressed rows "
            "(DecompressionTime: %v, RowCount: %v/%v, CompressedSize: %v, DecompressedSize: %v, UncompressedSize: %v)",
            decompressionTimer.GetElapsedTime(),
            newDecompressedValueCount,
            values.size(),
            currentCompressedSize,
            currentDecompressedSize,
            currentUncompressedSize);

        DecompressionTime_ += decompressionTimer.GetElapsedTime();

        YT_VERIFY(newDecompressedValueCount <= std::ssize(values));
        if (newDecompressedValueCount == std::ssize(values)) {
            Promise_.TrySet(std::move(Blobs_));
        } else {
            GetCurrentInvoker()->Invoke(BIND(
                &TDictionaryDecompressionSession::DoDecompressValues,
                MakeStrong(this),
                newDecompressedValueCount,
                Passed(std::move(values)),
                Passed(std::move(dictionaryIds)),
                Passed(std::move(chunkReadOptions)),
                Passed(std::move(decompressorsOrError))));
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TDictionaryDecompressionSession)

////////////////////////////////////////////////////////////////////////////////

IDictionaryDecompressionSessionPtr CreateDictionaryDecompressionSession(
    TWeakPtr<IDictionaryCompressionFactory> dictionaryCompressionFactory,
    TDictionaryCompressionSessionConfigPtr config,
    NLogging::TLogger logger)
{
    return New<TDictionaryDecompressionSession>(
        std::move(dictionaryCompressionFactory),
        std::move(config),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleDictionaryCompressionFactory
    : public IDictionaryCompressionFactory
{
public:
    TSimpleDictionaryCompressionFactory(
        IChunkFragmentReaderPtr chunkFragmentReader,
        TTableReaderConfigPtr readerConfig,
        TNameTablePtr nameTable,
        TChunkReaderHostPtr chunkReaderHost)
        : ChunkFragmentReader_(std::move(chunkFragmentReader))
        , ReaderConfig_(std::move(readerConfig))
        , NameTable_(std::move(nameTable))
        , ChunkReaderHost_(std::move(chunkReaderHost))
    { }

    TFuture<IDictionaryCompressionSessionPtr> MaybeCreateDictionaryCompressionSession(
        const TClientChunkReadOptions& /*chunkReadOptions*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    IDictionaryDecompressionSessionPtr CreateDictionaryDecompressionSession() override
    {
        return ::NYT::NTableClient::CreateDictionaryDecompressionSession(
            MakeWeak(this),
            ReaderConfig_,
            TableClientLogger);
    }

    TFuture<THashMap<TChunkId, TRowDictionaryDecompressor>> GetDecompressors(
        const TClientChunkReadOptions& chunkReadOptions,
        const THashSet<TChunkId>& dictionaryIds) override
    {
        std::vector<TChunkId> dictionaryIdList;
        std::vector<TFuture<TRowDigestedDictionary>> digestedDictionaryFutures;
        dictionaryIdList.reserve(dictionaryIds.size());
        digestedDictionaryFutures.reserve(dictionaryIds.size());

        for (auto dictionaryId : dictionaryIds) {
            dictionaryIdList.push_back(dictionaryId);
            digestedDictionaryFutures.push_back(ReadDigestedDictionary(
                dictionaryId,
                /*isDecompression*/ true,
                ChunkReaderHost_,
                ReaderConfig_,
                ReaderConfig_,
                chunkReadOptions,
                NameTable_,
                ChunkFragmentReader_,
                TableClientLogger));
        }

        return AllSucceeded(std::move(digestedDictionaryFutures)).Apply(BIND(
            [
                dictionaryIdList = std::move(dictionaryIdList)
            ] (const std::vector<TRowDigestedDictionary>& digestedDictionaries) {
                YT_VERIFY(dictionaryIdList.size() == digestedDictionaries.size());

                THashMap<TChunkId, TRowDictionaryDecompressor> result;
                for (int index = 0; index < std::ssize(dictionaryIdList); ++index) {
                    const auto& digestedDictionary = digestedDictionaries[index];
                    YT_VERIFY(std::holds_alternative<TRowDigestedDecompressionDictionary>(digestedDictionary));
                    auto rowDecompressor = CreateRowDictionaryDecompressor(
                        std::get<TRowDigestedDecompressionDictionary>(digestedDictionary));
                    EmplaceOrCrash(
                        result,
                        dictionaryIdList[index],
                        std::move(rowDecompressor));
                }

                return result;
            }));
    }

private:
    const IChunkFragmentReaderPtr ChunkFragmentReader_;
    const TTableReaderConfigPtr ReaderConfig_;
    const TNameTablePtr NameTable_;
    const TChunkReaderHostPtr ChunkReaderHost_;
};

////////////////////////////////////////////////////////////////////////////////

IDictionaryCompressionFactoryPtr CreateSimpleDictionaryCompressionFactory(
    IChunkFragmentReaderPtr chunkFragmentReader,
    TTableReaderConfigPtr readerConfig,
    TNameTablePtr nameTable,
    TChunkReaderHostPtr chunkReaderHost)
{
    return New<TSimpleDictionaryCompressionFactory>(
        std::move(chunkFragmentReader),
        std::move(readerConfig),
        std::move(nameTable),
        std::move(chunkReaderHost));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TRowDigestedDictionary> OnDictionaryMetaRead(
    const IChunkReaderPtr& chunkReader,
    TChunkId dictionaryId,
    bool isDecompression,
    TDictionaryCompressionSessionConfigPtr dictionaryReaderConfig,
    TClientChunkReadOptions chunkReadOptions,
    TWallTimer metaWaitTimer,
    const TNameTablePtr& nameTable,
    const IChunkFragmentReaderPtr& chunkFragmentReader,
    NLogging::TLogger logger,
    const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
{
    // Hold reader to prevent its destruction during meta read.
    Y_UNUSED(chunkReader);

    const auto& Logger = logger;

    if (!metaOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to read meta of a dictionary chunk")
            << TErrorAttribute("dictionary_id", dictionaryId)
            << TErrorAttribute("is_decompression", isDecompression)
            << TErrorAttribute("read_session_id", chunkReadOptions.ReadSessionId)
            << metaOrError;
    }

    chunkReadOptions.ChunkReaderStatistics->MetaWaitTime.fetch_add(
        metaWaitTimer.GetElapsedValue(),
        std::memory_order::relaxed);

    const auto& meta = metaOrError.Value();
    auto compressionDictionaryExt = GetProtoExtension<NTableClient::NProto::TCompressionDictionaryExt>(
        meta->extensions());

    std::vector<int> columnIdMapping;
    columnIdMapping.reserve(compressionDictionaryExt.column_infos_size());
    for (int index = 0; index < compressionDictionaryExt.column_infos_size(); ++index) {
        const auto& columnInfo = compressionDictionaryExt.column_infos(index);
        columnIdMapping.push_back(nameTable->GetIdOrThrow(columnInfo.stable_name()));
    }

    auto erasureCodec = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions()).erasure_codec();
    YT_VERIFY(CheckedEnumCast<NErasure::ECodec>(erasureCodec) == NErasure::ECodec::None);

    std::vector<IChunkFragmentReader::TChunkFragmentRequest> requests;
    requests.reserve(compressionDictionaryExt.column_infos_size());
    for (int index = 0; index < compressionDictionaryExt.column_infos_size(); ++index) {
        const auto& columnInfo = compressionDictionaryExt.column_infos(index);
        requests.push_back({
            .ChunkId = dictionaryId,
            .Length = columnInfo.length(),
            .BlockIndex = columnInfo.block_index(),
            .BlockOffset = columnInfo.block_offset(),
        });
    }

    auto hunkChunkReaderStatistics = chunkReadOptions.HunkChunkReaderStatistics;
    if (hunkChunkReaderStatistics) {
        chunkReadOptions.ChunkReaderStatistics = hunkChunkReaderStatistics->GetChunkReaderStatistics();
    }

    YT_LOG_DEBUG("Will read fragments of a dictionary chunk (FragmentCount: %v)",
        requests.size());

    return chunkFragmentReader->ReadFragments(
        chunkReadOptions,
        std::move(requests))
        .Apply(BIND(
            [
                =,
                logger = std::move(logger),
                columnIdMapping = std::move(columnIdMapping),
                dictionaryReaderConfig = std::move(dictionaryReaderConfig),
                chunkReadOptions = std::move(chunkReadOptions)
            ] (const TErrorOr<IChunkFragmentReader::TReadFragmentsResponse>& responseOrError) -> TRowDigestedDictionary
        {
            const auto& Logger = logger;

            if (!responseOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to read fragments of a dictionary chunk")
                    << TErrorAttribute("dictionary_id", dictionaryId)
                    << TErrorAttribute("is_decompression", isDecompression)
                    << TErrorAttribute("read_session_id", chunkReadOptions.ReadSessionId)
                    << responseOrError;
            }

            auto response = responseOrError.Value();
            YT_VERIFY(response.Fragments.size() == columnIdMapping.size());

            YT_LOG_DEBUG("Successfully read fragments of a dictionary chunk");

            if (hunkChunkReaderStatistics) {
                hunkChunkReaderStatistics->DataWeight() += GetByteSize(response.Fragments);
                hunkChunkReaderStatistics->RefValueCount() += std::ssize(response.Fragments);
                hunkChunkReaderStatistics->BackendReadRequestCount() += response.BackendReadRequestCount;
                hunkChunkReaderStatistics->BackendHedgingReadRequestCount() += response.BackendHedgingReadRequestCount;
                hunkChunkReaderStatistics->BackendProbingRequestCount() += response.BackendProbingRequestCount;
            }

            if (isDecompression) {
                TRowDigestedDecompressionDictionary rowDigestedDictionary;
                // NB: Columns that are missing in the dictionary will be ignored here
                // and no decompression will be performed either.
                for (int index = 0; index < std::ssize(columnIdMapping); ++index) {
                    YT_VERIFY(response.Fragments[index]);

                    auto digestedDictionary = GetDictionaryCompressionCodec()->CreateDigestedDecompressionDictionary(
                        response.Fragments[index]);
                    EmplaceOrCrash(
                        rowDigestedDictionary,
                        columnIdMapping[index],
                        std::move(digestedDictionary));
                }
                return rowDigestedDictionary;
            } else {
                TRowDigestedCompressionDictionary rowDigestedDictionary;
                // NB: Columns that are missing in the dictionary will be ignored here
                // and no compression will be performed either.
                for (int index = 0; index < std::ssize(columnIdMapping); ++index) {
                    YT_VERIFY(response.Fragments[index]);

                    auto digestedDictionary = GetDictionaryCompressionCodec()->CreateDigestedCompressionDictionary(
                        response.Fragments[index],
                        dictionaryReaderConfig->CompressionLevel);
                    EmplaceOrCrash(
                        rowDigestedDictionary,
                        columnIdMapping[index],
                        std::move(digestedDictionary));
                }
                return rowDigestedDictionary;
            }
        }));
}

TFuture<TRowDigestedDictionary> ReadDigestedDictionary(
    TChunkId dictionaryId,
    bool isDecompression,
    TChunkReaderHostPtr chunkReaderHost,
    TErasureReaderConfigPtr storeReaderConfig,
    TDictionaryCompressionSessionConfigPtr dictionaryReaderConfig,
    TClientChunkReadOptions chunkReadOptions,
    TNameTablePtr nameTable,
    IChunkFragmentReaderPtr chunkFragmentReader,
    NLogging::TLogger logger)
{
    logger.AddTag("ReadSessionId: %v, DictionaryId: %v, IsDecompression: %v",
        chunkReadOptions.ReadSessionId,
        dictionaryId,
        isDecompression);

    const auto& Logger = logger;

    YT_LOG_DEBUG("Will read meta of a dictionary chunk");

    TWallTimer metaWaitTimer;

    NChunkClient::NProto::TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), dictionaryId);

    auto chunkReader = CreateRemoteReader(
        chunkSpec,
        // Using this config here to read hunk chunk meta seems fine.
        std::move(storeReaderConfig),
        New<TRemoteReaderOptions>(),
        std::move(chunkReaderHost));

    return chunkReader->GetMeta(chunkReadOptions)
        .Apply(BIND(
            OnDictionaryMetaRead,
            std::move(chunkReader),
            dictionaryId,
            isDecompression,
            Passed(std::move(dictionaryReaderConfig)),
            Passed(std::move(chunkReadOptions)),
            metaWaitTimer,
            std::move(nameTable),
            std::move(chunkFragmentReader),
            Passed(std::move(logger))));
}

////////////////////////////////////////////////////////////////////////////////

TRowDictionaryDecompressor CreateRowDictionaryDecompressor(
    const TRowDigestedDecompressionDictionary& digestedDictionary)
{
    TRowDictionaryDecompressor rowDecompressor;
    for (const auto& [valueId, columnDigestedDictionary] : digestedDictionary) {
        EmplaceOrCrash(
            rowDecompressor,
            valueId,
            GetDictionaryCompressionCodec()->CreateDictionaryDecompressor(columnDigestedDictionary));
    }

    return rowDecompressor;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
