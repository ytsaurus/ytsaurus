#include "yt_file_source.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/table_client/blob_reader.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/folder/path.h>
#include <util/stream/file.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

static constexpr TStringBuf CypressFilePayloadName = "data";

struct TLockedYTFileSourceObject
{
    ITransactionPtr Transaction;
    EObjectType Type;
    TObjectId ObjectId;
    TRevision Revision;
    std::optional<i64> Size;
};

std::string ResolveCluster(
    const TRichYPath& path,
    const TFileSourceContextPtr& context)
{
    if (path.GetCluster()) {
        return *path.GetCluster();
    }
    THROW_ERROR_EXCEPTION_UNLESS(
        context->PipelinePath.GetCluster(),
        "Pipeline path must have a cluster to resolve YT file source path %v",
        path);
    return *context->PipelinePath.GetCluster();
}

void AbortTransaction(const ITransactionPtr& transaction)
{
    YT_UNUSED_FUTURE(transaction->Abort());
}

TLockedYTFileSourceObject LockYTFileSourceObject(
    const IClientPtr& client,
    const TYPath& path)
{
    auto transaction = WaitFor(client->StartTransaction(ETransactionType::Master))
        .ValueOrThrow();
    auto abortGuard = Finally([transaction] {
        AbortTransaction(transaction);
    });

    auto lockResult = WaitFor(transaction->LockNode(path, ELockMode::Snapshot))
        .ValueOrThrow();

    TGetNodeOptions options;
    options.Attributes = {
        "id",
        "type",
        "revision",
        "uncompressed_data_size",
        "dynamic",
        "content_revision",
        "schema",
    };
    auto node = ConvertToNode(WaitFor(transaction->GetNode(
        Format("#%v&", lockResult.NodeId),
        options))
            .ValueOrThrow());
    const auto& attributes = node->Attributes();

    auto objectId = attributes.Get<TObjectId>("id");
    THROW_ERROR_EXCEPTION_UNLESS(
        objectId == lockResult.NodeId,
        "YT file source snapshot lock returned inconsistent object identity")
        .With("locked_object_id", lockResult.NodeId)
        .With("actual_object_id", objectId);

    auto type = attributes.Get<EObjectType>("type");
    TRevision revision;
    std::optional<i64> size;
    switch (type) {
        case EObjectType::File:
            revision = attributes.Get<TRevision>("revision");
            size = attributes.Get<i64>("uncompressed_data_size");
            THROW_ERROR_EXCEPTION_UNLESS(
                *size >= 0,
                "YT file source size must be nonnegative");
            break;

        case EObjectType::Table: {
            THROW_ERROR_EXCEPTION_UNLESS(
                !attributes.Get<bool>("dynamic"),
                "YT file source table %v must be static",
                path);

            auto expectedSchema = GetYTFileSourceBlobTableSchema();
            auto actualSchema = attributes.Get<TTableSchemaPtr>("schema");
            THROW_ERROR_EXCEPTION_UNLESS(
                *actualSchema == *expectedSchema,
                "YT file source table %v has an incompatible schema",
                path)
                .With("expected_schema", expectedSchema)
                .With("actual_schema", actualSchema);
            revision = attributes.Get<TRevision>("content_revision");
            break;
        }

        default:
            THROW_ERROR_EXCEPTION(
                "YT file source path %v must resolve to a Cypress file or a BLOB table",
                path)
                .With("actual_type", type);
    }

    abortGuard.Release();
    return {
        .Transaction = std::move(transaction),
        .Type = type,
        .ObjectId = objectId,
        .Revision = revision,
        .Size = size,
    };
}

const TUnversionedValue& GetRowValue(
    TUnversionedRow row,
    int id,
    TStringBuf columnName)
{
    for (const auto& value : row) {
        if (value.Id == id) {
            return value;
        }
    }
    THROW_ERROR_EXCEPTION("YT BLOB table row has no column %Qv", columnName);
}

std::string GetStringValue(
    TUnversionedRow row,
    int id,
    TStringBuf columnName)
{
    const auto& value = GetRowValue(row, id, columnName);
    THROW_ERROR_EXCEPTION_UNLESS(
        value.Type == EValueType::String,
        "YT BLOB table column %Qv must have string values",
        columnName);
    return std::string(value.AsStringBuf());
}

i64 GetInt64Value(
    TUnversionedRow row,
    int id,
    TStringBuf columnName)
{
    const auto& value = GetRowValue(row, id, columnName);
    THROW_ERROR_EXCEPTION_UNLESS(
        value.Type == EValueType::Int64,
        "YT BLOB table column %Qv must have int64 values",
        columnName);
    return value.Data.Int64;
}

void ValidateLockedRevision(
    const TLockedYTFileSourceObject& object,
    const TYTFileSourceLocatorPtr& locator)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        object.Type == EObjectType::Table &&
            object.ObjectId == locator->ObjectId &&
            object.Revision == locator->Revision,
        "YT BLOB table changed between discovery and download")
        .With("expected_object_id", locator->ObjectId)
        .With("actual_object_id", object.ObjectId)
        .With("expected_content_revision", locator->Revision)
        .With("actual_content_revision", object.Revision)
        .With("actual_type", object.Type);
}

void DownloadBlobTable(
    const TLockedYTFileSourceObject& object,
    const std::string& stagingDirectory)
{
    auto path = TRichYPath(Format("#%v", object.ObjectId));
    path.SetColumns({"filename", TBlobTableSchema::PartIndexColumn, TBlobTableSchema::DataColumn});
    auto reader = WaitFor(object.Transaction->CreateTableReader(path))
        .ValueOrThrow();
    auto nameTable = reader->GetNameTable();
    auto fileNameId = nameTable->GetIdOrRegisterName("filename");
    auto partIndexId = nameTable->GetIdOrRegisterName(TBlobTableSchema::PartIndexColumn);
    auto dataId = nameTable->GetIdOrRegisterName(TBlobTableSchema::DataColumn);

    std::string currentFileName;
    i64 previousPartIndex = -1;
    THolder<TFileOutput> output;

    while (auto batch = ReadRowBatch(reader, TRowBatchReadOptions{.MaxRowsPerRead = 1024})) {
        for (auto row : batch->MaterializeRows()) {
            auto fileName = GetStringValue(row, fileNameId, "filename");
            auto partIndex = GetInt64Value(row, partIndexId, TBlobTableSchema::PartIndexColumn);
            const auto& data = GetRowValue(row, dataId, TBlobTableSchema::DataColumn);
            THROW_ERROR_EXCEPTION_UNLESS(
                data.Type == EValueType::String,
                "YT BLOB table column %Qv must have string values",
                TBlobTableSchema::DataColumn);
            ValidateFileSourceName(fileName);

            if (fileName != currentFileName) {
                THROW_ERROR_EXCEPTION_UNLESS(
                    currentFileName.empty() || fileName > currentFileName,
                    "YT BLOB table filenames are not strictly ordered");
                THROW_ERROR_EXCEPTION_UNLESS(
                    partIndex == 0,
                    "YT BLOB table file %Qv must start with part index 0",
                    fileName);
                if (output) {
                    output->Finish();
                }
                currentFileName = fileName;
                previousPartIndex = -1;
                output = MakeHolder<TFileOutput>(
                    (TFsPath(stagingDirectory) / currentFileName).GetPath());
            }

            THROW_ERROR_EXCEPTION_UNLESS(
                partIndex == previousPartIndex + 1,
                "YT BLOB table part indexes must be consecutive")
                .With("file_name", fileName)
                .With("expected_part_index", previousPartIndex + 1)
                .With("actual_part_index", partIndex);
            previousPartIndex = partIndex;
            output->Write(data.AsStringBuf().data(), data.AsStringBuf().size());
        }
    }

    if (output) {
        output->Finish();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TYTFileSourceLocator::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster", &TThis::Cluster);
    registrar.Parameter("object_path", &TThis::ObjectPath);
    registrar.Parameter("object_id", &TThis::ObjectId);
    registrar.Parameter("revision", &TThis::Revision);
    registrar.Parameter("object_kind", &TThis::ObjectKind);
}

TTableSchemaPtr GetYTFileSourceBlobTableSchema()
{
    TBlobTableSchema schema;
    schema.BlobIdColumns.emplace_back("filename", EValueType::String);
    return schema.ToTableSchema();
}

TFileSourceRevisionPtr MakeYTFileSourceRevision(
    TStringBuf fileSourceClassName,
    const TRichYPath& originalPath,
    const std::string& cluster,
    TObjectId objectId,
    TRevision revision,
    i64 size)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        size >= 0,
        "YT file source size must be nonnegative");

    auto locator = New<TYTFileSourceLocator>();
    locator->Cluster = cluster;
    locator->ObjectPath = Format("#%v", objectId);
    locator->ObjectId = objectId;
    locator->Revision = revision;
    locator->ObjectKind = EYTFileSourceObjectKind::CypressFile;

    auto result = New<TFileSourceRevision>();
    result->FileSourceClassName = std::string(fileSourceClassName);
    result->ObjectId = NFileStorage::TFileStorageObjectId(
        Format("yt_file:v1:%v:%v:%v", cluster, objectId, revision));
    result->DisplayVersion = Format("%v@%v", originalPath, revision);
    result->Size = size;
    result->Locator = ConvertToNode(locator)->AsMap();
    return result;
}

TFileSourceRevisionPtr MakeYTBlobTableFileSourceRevision(
    TStringBuf fileSourceClassName,
    const TRichYPath& originalPath,
    const std::string& cluster,
    TObjectId objectId,
    TRevision contentRevision)
{
    auto locator = New<TYTFileSourceLocator>();
    locator->Cluster = cluster;
    locator->ObjectPath = Format("#%v", objectId);
    locator->ObjectId = objectId;
    locator->Revision = contentRevision;
    locator->ObjectKind = EYTFileSourceObjectKind::BlobTable;

    auto result = New<TFileSourceRevision>();
    result->FileSourceClassName = std::string(fileSourceClassName);
    result->ObjectId = NFileStorage::TFileStorageObjectId(
        Format("yt_blob_table:v1:%v:%v:%v", cluster, objectId, contentRevision));
    result->DisplayVersion = Format("%v@%v", originalPath, contentRevision);
    result->Size = std::nullopt;
    result->Locator = ConvertToNode(locator)->AsMap();
    return result;
}

TFuture<TFileSourceRevisionPtr> DiscoverYTFileSource(
    const TFileSourceContextPtr& context,
    TStringBuf fileSourceClassName,
    const TRichYPath& path)
{
    auto cluster = ResolveCluster(path, context);
    auto client = context->ClientsCache->GetClient(cluster);
    auto object = LockYTFileSourceObject(client, path.GetPath());
    auto abortGuard = Finally([transaction = object.Transaction] {
        AbortTransaction(transaction);
    });

    switch (object.Type) {
        case EObjectType::File:
            return MakeFuture(MakeYTFileSourceRevision(
                fileSourceClassName,
                path,
                cluster,
                object.ObjectId,
                object.Revision,
                *object.Size));

        case EObjectType::Table:
            return MakeFuture(MakeYTBlobTableFileSourceRevision(
                fileSourceClassName,
                path,
                cluster,
                object.ObjectId,
                object.Revision));

        default:
            YT_ABORT();
    }
}

TFuture<void> DownloadYTFile(
    const TFileSourceContextPtr& context,
    const TFileSourceRevisionPtr& revision,
    const std::string& stagingDirectory)
{
    auto locator = ConvertTo<TYTFileSourceLocatorPtr>(revision->Locator);
    auto client = context->ClientsCache->GetClient(locator->Cluster);

    switch (locator->ObjectKind) {
        case EYTFileSourceObjectKind::CypressFile: {
            auto reader = WaitFor(client->CreateFileReader(locator->ObjectPath)).ValueOrThrow();
            THROW_ERROR_EXCEPTION_UNLESS(
                reader->GetId() == locator->ObjectId && reader->GetRevision() == locator->Revision,
                "YT file changed between discovery and download")
                .With("expected_object_id", locator->ObjectId)
                .With("actual_object_id", reader->GetId())
                .With("expected_revision", locator->Revision)
                .With("actual_revision", reader->GetRevision());

            TFileOutput output((TFsPath(stagingDirectory) / CypressFilePayloadName).GetPath());
            while (auto block = WaitFor(reader->Read()).ValueOrThrow()) {
                output.Write(block.Begin(), block.Size());
            }
            output.Finish();
            break;
        }

        case EYTFileSourceObjectKind::BlobTable: {
            auto object = LockYTFileSourceObject(client, locator->ObjectPath);
            auto abortGuard = Finally([transaction = object.Transaction] {
                AbortTransaction(transaction);
            });
            ValidateLockedRevision(object, locator);
            DownloadBlobTable(object, stagingDirectory);
            break;
        }
    }

    return MakeFuture<void>(TError());
}

////////////////////////////////////////////////////////////////////////////////

void TYTFileSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

TFuture<TFileSourceRevisionPtr> TYTFileSource::Discover()
{
    auto path = GetParameters()->Path;
    return DiscoverYTFileSource(
        GetContext(),
        TypeName<TYTFileSource>(),
        path);
}

TFuture<void> TYTFileSource::Download(
    const TFileSourceRevisionPtr& revision,
    const std::string& stagingDirectory)
{
    return DownloadYTFile(GetContext(), revision, stagingDirectory);
}

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_FILE_SOURCE(TYTFileSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
