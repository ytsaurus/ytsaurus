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

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<TDistributedWriteSessionWithCookies> TClient::StartDistributedWriteSession(
    const TRichYPath& richPath,
    const TDistributedWriteSessionStartOptions& options)
{
    const auto& path = richPath.GetPath();

    // NB(arkady-e1ppa): Started transaction is discarded in this scope
    // and is expected to be attached by the user, thus we must not abort it.
    TTransactionStartOptions transactionStartOptions{
        .AutoAbort = false,
    };
    if (options.TransactionId) {
        transactionStartOptions.ParentId = options.TransactionId;
        transactionStartOptions.Ping = true;
        transactionStartOptions.PingAncestors = options.PingAncestors;
    }

    auto transaction = WaitFor(
        StartTransaction(
            NTransactionClient::ETransactionType::Master,
            transactionStartOptions))
                .ValueOrThrow();

    TUserObject userObject(path);

    GetUserObjectBasicAttributes(
        MakeStrong(this),
        {&userObject},
        transaction->GetId(),
        Logger,
        EPermission::Write);

    if (userObject.Type != EObjectType::Table) {
        THROW_ERROR_EXCEPTION(
            "Invalid type of %v: expected %Qlv, actual %Qlv",
            path,
            EObjectType::Table,
            userObject.Type);
    }

    auto objectId = userObject.ObjectId;
    auto nativeCellTag = CellTagFromId(objectId);
    auto externalCellTag = userObject.ExternalCellTag;
    auto objectIdPath = FromObjectId(objectId);

    TTableSchemaPtr chunkSchema;
    TTableUploadOptions tableUploadOptions;
    INodePtr nodeWithAttributes;

    {
        YT_LOG_DEBUG("Requesting extended table attributes");

        nodeWithAttributes = NTableClient::NDetail::GetTableAttributes(
            MakeStrong(this),
            path,
            externalCellTag,
            objectIdPath,
            userObject);

        const auto& attributes = nodeWithAttributes->Attributes();

        if (attributes.Get<bool>("dynamic")) {
            THROW_ERROR_EXCEPTION("\"distributed_write_sessions\" API is not supported for dynamic tables; use \"insert_rows\" instead");
        }

        tableUploadOptions = GetTableUploadOptions(
            richPath,
            attributes,
            attributes.Get<TTableSchemaPtr>("schema"),
            attributes.Get<i64>("row_count"));

        chunkSchema = NTableClient::NDetail::GetChunkSchema(richPath, tableUploadOptions);

        YT_LOG_DEBUG("Extended attributes received (Attributes: %v)", ConvertToYsonString(attributes, EYsonFormat::Text));
    }

    auto [chunkSchemaId, uploadTransactionId] = NTableClient::NDetail::BeginTableUpload(
        MakeStrong(this),
        path,
        nativeCellTag,
        objectIdPath,
        transaction->GetId(),
        tableUploadOptions,
        chunkSchema,
        Logger,
        /*setUploadTransactionTimeout*/ false);

    auto [writerLastKey, rootChunkListId, maxHeavyColumns] = NTableClient::NDetail::GetTableUploadParams(
        MakeStrong(this),
        path,
        externalCellTag,
        objectIdPath,
        uploadTransactionId,
        tableUploadOptions,
        Logger);

    auto timestamp = WaitFor(GetNativeConnection()->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    auto session = TDistributedWriteSession(
        /*mainTransactionId*/ transaction->GetId(),
        uploadTransactionId,
        rootChunkListId,
        std::move(richPath),
        objectId,
        /*externalCellTag*/ externalCellTag,
        chunkSchemaId,
        std::move(chunkSchema),
        static_cast<bool>(writerLastKey) ? std::optional(std::move(writerLastKey)) : std::nullopt,
        maxHeavyColumns,
        timestamp,
        nodeWithAttributes->Attributes());

    std::vector<TSignedWriteFragmentCookiePtr> cookies;
    cookies.reserve(options.CookieCount);

    for (int i = 0; i < options.CookieCount; ++i) {
        cookies.emplace_back(TSignedWriteFragmentCookiePtr(DummySignatureGenerator_->Sign(ConvertToYsonString(session.CookieFromThis()))));
    }

    TDistributedWriteSessionWithCookies result;
    result.Session = TSignedDistributedWriteSessionPtr(DummySignatureGenerator_->Sign(ConvertToYsonString(session)));
    result.Cookies = std::move(cookies);

    return MakeFuture(std::move(result));
}

TFuture<void> TClient::FinishDistributedWriteSession(
    const TDistributedWriteSessionWithResults& sessionWithResults,
    const TDistributedWriteSessionFinishOptions& options)
{
    YT_VERIFY(sessionWithResults.Session);

    auto session = ConvertTo<TDistributedWriteSession>(sessionWithResults.Session.Underlying()->Payload());

    const auto& patchInfo = session.PatchInfo;
    const auto& path = patchInfo.RichPath.GetPath();

    auto attributes = IAttributeDictionary::FromMap(patchInfo.TableAttributes->AsMap());

    auto transaction = AttachTransaction(session.MainTransactionId, TTransactionAttachOptions{
        .AutoAbort = true,
    });

    const auto tableUploadOptions = GetTableUploadOptions(
        patchInfo.RichPath,
        *attributes,
        attributes->Get<TTableSchemaPtr>("schema"),
        attributes->Get<i64>("row_count"));

    NChunkClient::NProto::TDataStatistics dataStatistics = {};

    // Attach chunk lists part.
    {
        YT_LOG_INFO(
            "Attaching participants' chunks (Path: %v)",
            path);

        auto channel = GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            patchInfo.ExternalCellTag);
        TChunkServiceProxy proxy(channel);

        // Split large outputs into separate requests.
        NChunkClient::NProto::TReqAttachChunkTrees* req = nullptr;
        TChunkServiceProxy::TReqExecuteBatchPtr batchReq;

        auto flushRequest = [&] (bool requestStatistics) {
            if (!batchReq) {
                return;
            }

            if (req) {
                req->set_request_statistics(requestStatistics);
                req = nullptr;
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Error attaching chunks to output table %v",
                path);

            const auto& batchRsp = batchRspOrError.Value();
            const auto& subresponses = batchRsp->attach_chunk_trees_subresponses();

            if (requestStatistics) {
                for (const auto& rsp : subresponses) {
                    dataStatistics += rsp.statistics();
                }
            }

            batchReq.Reset();
        };

        int currentRequestSize = 0;
        THashSet<TChunkTreeId> addedChunkTrees;

        auto addChunkTree = [&] (TChunkTreeId chunkTreeId) {
            if (batchReq && currentRequestSize >= options.MaxChildrenPerAttachRequest) {
                // NB: Static tables do not need statistics for intermediate requests.
                flushRequest(false);
                currentRequestSize = 0;
            }

            ++currentRequestSize;

            if (!req) {
                if (!batchReq) {
                    batchReq = proxy.ExecuteBatch();
                    GenerateMutationId(batchReq);
                    SetSuppressUpstreamSync(&batchReq->Header(), true);
                    // COMPAT(shakurov): prefer proto ext (above).
                    batchReq->set_suppress_upstream_sync(true);
                }
                req = batchReq->add_attach_chunk_trees_subrequests();
                ToProto(req->mutable_parent_id(), session.RootChunkListId);
            }

            ToProto(req->add_child_ids(), chunkTreeId);
        };

        std::vector<TWriteFragmentResult> writeResults;
        writeResults.reserve(std::ssize(sessionWithResults.Results));
        for (const auto& signedResult : sessionWithResults.Results) {
            YT_VERIFY(signedResult);
            writeResults.push_back(ConvertTo<TWriteFragmentResult>(signedResult.Underlying()->Payload()));
        }

        if (tableUploadOptions.TableSchema->IsSorted()) {
            // Sorted output generated by user operation requires rearranging.

            SortAndValidateDistributedWriteResults(
                &writeResults,
                patchInfo,
                tableUploadOptions,
                Logger);

            for (const auto& writeResult : writeResults) {
                addChunkTree(writeResult.ChunkListId);
            }
        } else {
            YT_LOG_DEBUG(
                "Attaching chunk tree ids in an arbitrary order (ChunkTreeCount: %v, Table: %v)",
                std::ssize(writeResults),
                path);

            for (const auto& writeResult : writeResults) {
                if (!addedChunkTrees.insert(writeResult.ChunkListId).second) {
                    THROW_ERROR_EXCEPTION("Duplicate chunk list ids")
                        << TErrorAttribute("session_id", writeResult.SessionId)
                        << TErrorAttribute("cookie_id", writeResult.CookieId)
                        << TErrorAttribute("chunk_list_id", writeResult.ChunkListId);
                }
                addChunkTree(writeResult.ChunkListId);
            }
        }

        // NB: Don't forget to ask for the statistics in the last request.
        flushRequest(true);

        YT_LOG_INFO(
            "Distributed writers' chunks attached (Path: %v, Statistics: %v)",
            path,
            dataStatistics);
    }

    NYT::NTableClient::NDetail::EndTableUpload(
        MakeStrong(this),
        path,
        CellTagFromId(patchInfo.ObjectId),
        FromObjectId(patchInfo.ObjectId),
        session.UploadTransactionId,
        tableUploadOptions,
        std::move(dataStatistics));

    return transaction->Commit().AsVoid();
}

TFuture<ITableFragmentWriterPtr> TClient::CreateTableFragmentWriter(
    const TSignedWriteFragmentCookiePtr& signedCookie,
    const TTableFragmentWriterOptions& options)
{
    YT_VERIFY(signedCookie);

    auto cookie = ConvertTo<TWriteFragmentCookie>(signedCookie.Underlying()->Payload());

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
