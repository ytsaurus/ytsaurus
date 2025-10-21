#include "client_impl.h"

#include "config.h"

#include <yt/yt/client/api/distributed_table_session.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/api/native/distributed_write_facade_base.h>

#include <yt/yt/client/table_client/table_upload_options.h>

#include <yt/yt/client/signature/generator.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace NTableClient::NDetail {

// declaration from schemaless_chunk_writer.cpp
TTableSchemaPtr GetChunkSchema(
    const TRichYPath& richPath,
    const TTableUploadOptions& options);

////////////////////////////////////////////////////////////////////////////////

INodePtr GetTableAttributes(
    const NApi::NNative::IClientPtr& client,
    const TRichYPath& path,
    TCellTag externalCellTag,
    const NYPath::TYPath& objectIdPath,
    const TUserObject& userObject);

////////////////////////////////////////////////////////////////////////////////

std::tuple<TMasterTableSchemaId, TTransactionId> BeginTableUpload(
    const NApi::NNative::IClientPtr& client,
    const TRichYPath path,
    TCellTag nativeCellTag,
    NYPath::TYPath objectIdPath,
    TTransactionId transactionId,
    const TTableUploadOptions& tableUploadOptions,
    const TTableSchemaPtr& chunkSchema,
    const NLogging::TLogger& Logger,
    bool setUploadTransactionTimeout);

////////////////////////////////////////////////////////////////////////////////

std::tuple<TLegacyOwningKey, TChunkListId, int> GetTableUploadParams(
    const NApi::NNative::IClientPtr& client,
    const TRichYPath path,
    TCellTag externalCellTag,
    NYPath::TYPath objectIdPath,
    TTransactionId uploadTransactionId,
    const TTableUploadOptions& tableUploadOptions,
    const NLogging::TLogger& Logger);

////////////////////////////////////////////////////////////////////////////////

void EndTableUpload(
    const NApi::NNative::IClientPtr& client,
    const TRichYPath& path,
    TCellTag nativeCellTag,
    TYPath objectIdPath,
    TTransactionId transactionId,
    const TTableUploadOptions& tableUploadOptions,
    NChunkClient::NProto::TDataStatistics dataStatistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient::NDetail

////////////////////////////////////////////////////////////////////////////////

namespace NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

namespace {

void SortAndValidateDistributedWriteResults(
    TNonNullPtr<std::vector<TWriteFragmentResult>> results,
    const TTableWriterPatchInfo& patchInfo,
    const TTableUploadOptions& tableUploadOptions,
    const NLogging::TLogger& Logger)
{
    const auto& path = patchInfo.RichPath.GetPath();
    YT_LOG_DEBUG(
        "Sorting output chunk tree ids by boundary keys (ChunkTreeCount: %v, Table: %v)",
        std::ssize(*results),
        path);

    if (results->empty()) {
        return;
    }

    YT_VERIFY(tableUploadOptions.TableSchema->IsSorted());
    const auto& comparator = tableUploadOptions.TableSchema->ToComparator();
    auto asKey = [] (const auto& key) {
        return TKey::FromRow(key);
    };
    auto lastKey = patchInfo.WriterLastKey
        ? asKey(*patchInfo.WriterLastKey)
        : TKey{};

    std::stable_sort(
        std::begin(*results),
        std::end(*results),
        [&] (const TWriteFragmentResult& lhs, const TWriteFragmentResult& rhs) -> bool {
            auto lhsMinKey = asKey(lhs.MinBoundaryKey);
            auto lhsMaxKey = asKey(lhs.MaxBoundaryKey);
            auto rhsMinKey = asKey(rhs.MinBoundaryKey);
            auto rhsMaxKey = asKey(rhs.MaxBoundaryKey);
            auto minKeyResult = comparator.CompareKeys(lhsMinKey, rhsMinKey);
            if (minKeyResult != 0) {
                return minKeyResult < 0;
            }
            return comparator.CompareKeys(lhsMaxKey, rhsMaxKey) < 0;
        });

    if (tableUploadOptions.UpdateMode == EUpdateMode::Append &&
        lastKey)
    {
        YT_LOG_DEBUG(
            "Comparing table last key against first chunk min key (LastKey: %v, MinKey: %v, Comparator: %v)",
            lastKey,
            std::begin(*results)->MinBoundaryKey,
            comparator);

        int cmp = comparator.CompareKeys(
            TKey::FromRow(std::begin(*results)->MinBoundaryKey),
            lastKey);

        if (cmp < 0) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::SortOrderViolation,
                "Output table %v is not sorted: key ranges overlap with original table",
                path)
                << TErrorAttribute("table_max_key", lastKey)
                << TErrorAttribute("min_key", std::begin(*results)->MinBoundaryKey)
                << TErrorAttribute("comparator", comparator);
        }

        if (cmp == 0 && patchInfo.ChunkSchema->IsUniqueKeys()) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::SortOrderViolation,
                "Output table %v contains duplicate keys: key ranges overlap with original table",
                path)
                << TErrorAttribute("table_max_key", lastKey)
                << TErrorAttribute("min_key", std::begin(*results)->MinBoundaryKey)
                << TErrorAttribute("comparator", comparator);
        }
    }

    for (auto current = std::begin(*results); current != std::end(*results); ++current) {
        auto next = current + 1;
        if (next == std::end(*results)) {
            break;
        }

        int cmp = comparator.CompareKeys(asKey(next->MinBoundaryKey), asKey(current->MaxBoundaryKey));

        if (cmp < 0) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::SortOrderViolation,
                "Output table %v is not sorted: key ranges have overlapping key ranges",
                path)
                << TErrorAttribute("current_range_max_key", current->MaxBoundaryKey)
                << TErrorAttribute("next_range_min_key", next->MinBoundaryKey)
                << TErrorAttribute("comparator", comparator);
        }

        if (cmp == 0 && patchInfo.ChunkSchema->IsUniqueKeys()) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::UniqueKeyViolation,
                "Output table %v contains duplicate keys: key ranges have overlapping key ranges",
                path)
                << TErrorAttribute("current_range_max_key", current->MaxBoundaryKey)
                << TErrorAttribute("next_range_min_key", next->MinBoundaryKey)
                << TErrorAttribute("comparator", comparator);
        }
    }
}

struct TDistributedTableSessionTraits {
    using TStartOptions = TDistributedWriteSessionStartOptions;
    using TPingOptions = TDistributedWriteSessionPingOptions;
    using TFinishOptions = TDistributedWriteSessionFinishOptions;

    using TSession = TDistributedWriteSession;
    using TWriteFragmentResult = TWriteFragmentResult;

    using TSessionWithCookies = TDistributedWriteSessionWithCookies;
    using TSessionWithResults = TDistributedWriteSessionWithResults;

    using TSignedSessionPtr = TSignedDistributedWriteSessionPtr;
    using TSignedCookiePtr = TSignedWriteFragmentCookiePtr;

    inline static const EObjectType ObjectType = EObjectType::Table;
};

static_assert(CDistributedWriteFacadeTraits<TDistributedTableSessionTraits>,
    "TDistributedTableSessionTraits must follow CDistributedWriteFacadeTraits concept");

class TDistributedWriteTableStartFacade
    : public TDistributedWriteStartFacadeBase<TDistributedWriteTableStartFacade, TDistributedTableSessionTraits>
{
    using TBase = TDistributedWriteStartFacadeBase<TDistributedWriteTableStartFacade, TDistributedTableSessionTraits>;
    using TTraits = TDistributedTableSessionTraits;

    friend class TDistributedWriteStartFacadeBase<TDistributedWriteTableStartFacade, TDistributedTableSessionTraits>;

public:
    TDistributedWriteTableStartFacade(
        const IClientPtr& client,
        const NLogging::TLogger& logger)
        : TBase(client, logger)
        , Client_(client)
        , Logger(logger)
    { }

protected:
    INodePtr RequestExtendedObjectAttributes(
        const TRichYPath& path,
        TCellTag externalCellTag,
        const TYPath& objectIdPath,
        const TUserObject& userObject)
    {
        auto nodeWithAttributes = NTableClient::NDetail::GetTableAttributes(
            Client_,
            path.GetPath(),
            externalCellTag,
            objectIdPath,
            userObject);

        const auto& attributes = nodeWithAttributes->Attributes();
        if (attributes.Get<bool>("dynamic")) {
            THROW_ERROR_EXCEPTION("\"distributed_write_sessions\" API is not supported for dynamic tables; use \"insert_rows\" instead");
        }

        TableUploadOptions_ = GetTableUploadOptions(
            path,
            attributes,
            attributes.Get<TTableSchemaPtr>("schema"),
            attributes.Get<i64>("row_count"));

        ChunkSchema_ = NTableClient::NDetail::GetChunkSchema(path, TableUploadOptions_);

        return nodeWithAttributes;
    }

    std::tuple<TMasterTableSchemaId, TTransactionId> BeginUpload(
        const TRichYPath& path,
        TCellTag nativeCellTag,
        const TYPath& objectIdPath,
        TTransactionId transactionId)
    {
        return NTableClient::NDetail::BeginTableUpload(
            Client_,
            path.GetPath(),
            nativeCellTag,
            objectIdPath,
            transactionId,
            TableUploadOptions_,
            ChunkSchema_,
            Logger,
            /*setUploadTransactionTimeout*/ false);
    }

    TChunkListId RequestUploadParameters(
        const TRichYPath& path,
        TCellTag externalCellTag,
        const TYPath& objectIdPath,
        TTransactionId uploadTransactionId)
    {
        auto [writerLastKey, rootChunkListId, maxHeavyColumns] = NTableClient::NDetail::GetTableUploadParams(
            Client_,
            path.GetPath(),
            externalCellTag,
            objectIdPath,
            uploadTransactionId,
            TableUploadOptions_,
            Logger);

        WriterLastKey_ = std::move(writerLastKey);
        MaxHeavyColumns_ = maxHeavyColumns;

        return rootChunkListId;
    }

    TTraits::TSession CreateSession(
        TTransactionId masterTransactionId,
        TTransactionId uploadTransactionId,
        TChunkListId rootChunkListId,
        const TRichYPath& path,
        TObjectId objectId,
        TCellTag externalCellTag,
        NTableClient::TMasterTableSchemaId chunkSchemaId,
        TTimestamp timestamp,
        INodePtr attributes)
    {
        return TDistributedWriteSession(
            masterTransactionId,
            uploadTransactionId,
            rootChunkListId,
            path,
            objectId,
            externalCellTag,
            chunkSchemaId,
            ChunkSchema_,
            static_cast<bool>(WriterLastKey_) ? std::optional(WriterLastKey_) : std::nullopt,
            MaxHeavyColumns_,
            timestamp,
            attributes->Attributes());
    }

private:
    const IClientPtr Client_;
    const NLogging::TLogger Logger;

    // Additional data to be reused within hooks
    TTableSchemaPtr ChunkSchema_;
    TTableUploadOptions TableUploadOptions_;
    TUnversionedOwningRow WriterLastKey_;
    int MaxHeavyColumns_;
};

class TDistributedWriteTablePingFacade
    : public TDistributedWritePingFacadeBase<TDistributedWriteTablePingFacade, TDistributedTableSessionTraits>
{
    using TBase = TDistributedWritePingFacadeBase<TDistributedWriteTablePingFacade, TDistributedTableSessionTraits>;
    using TTraits = TDistributedTableSessionTraits;

    friend class TDistributedWritePingFacadeBase<TDistributedWriteTablePingFacade, TDistributedTableSessionTraits>;

public:
    explicit TDistributedWriteTablePingFacade(const IClientPtr& client)
        : TBase(client)
    { }

protected:
    TTransactionId GetMasterTransaction(const TTraits::TSession& session)
    {
        return session.MainTransactionId;
    }
};

class TDistributedWriteTableFinishFacade
    : public TDistributedWriteFinishFacadeBase<TDistributedWriteTableFinishFacade, TDistributedTableSessionTraits>
{
    using TBase = TDistributedWriteFinishFacadeBase<TDistributedWriteTableFinishFacade, TDistributedTableSessionTraits>;
    using TTraits = TDistributedTableSessionTraits;

    friend class TDistributedWriteFinishFacadeBase<TDistributedWriteTableFinishFacade, TDistributedTableSessionTraits>;

public:
    TDistributedWriteTableFinishFacade(
        const IClientPtr& client,
        const NLogging::TLogger& logger)
        : TBase(client, logger)
        , Client_(client)
        , Logger(logger)
    { }

protected:
    TTransactionId GetMasterTransaction(const TTraits::TSession& session) {
        return session.MainTransactionId;
    }

    TObjectId GetObjectId(const TTraits::TSession& session) {
        return session.PatchInfo.ObjectId;
    }

    NYPath::TRichYPath GetPath(const TTraits::TSession& session) {
        return session.PatchInfo.RichPath;
    }

    NObjectClient::TCellTag GetExternalCellTag(const TTraits::TSession& session) {
        return session.PatchInfo.ExternalCellTag;
    }

    void SortResults(
        TNonNullPtr<std::vector<typename TTraits::TWriteFragmentResult>> results,
        const TTraits::TSession& session)
    {
        auto tableUploadOptions = GetTableUploadOptions(session);
        // Sorted output generated by user operation requires rearranging.
        if (tableUploadOptions.TableSchema->IsSorted()) {
            SortAndValidateDistributedWriteResults(
                results,
                session.PatchInfo,
                tableUploadOptions,
                Logger);
        }
    }

    void EndUpload(
        const TTraits::TSession& session,
        const NChunkClient::NProto::TDataStatistics& dataStatistics)
    {
        const auto& patchInfo = session.PatchInfo;
        auto tableUploadOptions = GetTableUploadOptions(session);

        NTableClient::NDetail::EndTableUpload(
            Client_,
            patchInfo.RichPath,
            CellTagFromId(patchInfo.ObjectId),
            FromObjectId(patchInfo.ObjectId),
            session.UploadTransactionId,
            tableUploadOptions,
            dataStatistics);
    }

private:
    const IClientPtr Client_;
    const NLogging::TLogger Logger;

    TTableUploadOptions GetTableUploadOptions(const TTraits::TSession& session)
    {
        const auto& patchInfo = session.PatchInfo;
        auto attributes = IAttributeDictionary::FromMap(patchInfo.TableAttributes->AsMap());

        return NYT::GetTableUploadOptions(
            patchInfo.RichPath,
            *attributes,
            attributes->Get<TTableSchemaPtr>("schema"),
            attributes->Get<i64>("row_count"));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TDistributedWriteSessionWithCookies TClient::DoStartDistributedWriteSession(
    const TRichYPath& richPath,
    const TDistributedWriteSessionStartOptions& options)
{
    TDistributedWriteTableStartFacade facade(
        MakeStrong(this),
        Logger);
    return facade.StartSession(richPath, options);
}

void TClient::DoPingDistributedWriteSession(
    TSignedDistributedWriteSessionPtr session,
    const TDistributedWriteSessionPingOptions& options)
{
    TDistributedWriteTablePingFacade facade(
        MakeStrong(this));
    facade.PingSession(session, options);
}

void TClient::DoFinishDistributedWriteSession(
    const TDistributedWriteSessionWithResults& sessionWithResults,
    const TDistributedWriteSessionFinishOptions& options)
{
    TDistributedWriteTableFinishFacade facade(
        MakeStrong(this),
        Logger);
    facade.FinishSession(sessionWithResults, options);
}

TFuture<ITableFragmentWriterPtr> TClient::CreateTableFragmentWriter(
    const TSignedWriteFragmentCookiePtr& signedCookie,
    const TTableFragmentWriterOptions& options)
{
    YT_VERIFY(signedCookie);

    auto cookie = ConvertTo<TWriteFragmentCookie>(TYsonStringBuf(signedCookie.Underlying()->Payload()));

    auto nameTable = New<TNameTable>();
    nameTable->SetEnableColumnNameValidation();

    auto writerOptions = New<NTableClient::TTableWriterOptions>();
    writerOptions->EnableValidationOptions(/*validateAnyIsValidYson*/ options.ValidateAnyIsValidYson);

    auto asyncSchemalessWriter = CreateSchemalessTableFragmentWriter(
        options.Config ? options.Config : New<TTableWriterConfig>(),
        writerOptions,
        cookie,
        nameTable,
        this,
        /*localHostName*/ std::string{}, // Locality is not important during table upload.
        cookie.MainTransactionId,
        /*writeBlocksOptions*/ {});

    return asyncSchemalessWriter.ApplyUnique(BIND([] (IUnversionedTableFragmentWriterPtr&& schemalessWriter) {
        return CreateApiFromSchemalessWriterAdapter(std::move(schemalessWriter));
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi::NNative

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
