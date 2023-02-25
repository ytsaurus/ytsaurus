#include "api.h"

#include <yt/cpp/mapreduce/library/blob/protos/info.pb.h>
#include <yt/cpp/mapreduce/library/blob/protos/row.pb.h>

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/library/table_schema/protobuf.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/protobuf/yql/descriptor.h>

#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/buffer.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/system/fs.h>

static constexpr auto CHUNK_SIZE_IN_BYTES = ui64{15} * 1024 * 1024;

void NYtBlob::Upload(
    const TString& path, const TString& filename,
    const ::NYT::TYPath& table, NYT::IClientBasePtr client) {

    TFileInput in{path};
    Upload(in, filename, table, client);
}

static void Upload(
    IInputStream& in, const TString& filename,
    NYT::TTableWriterPtr<NYtBlob::TBlobChunk> w) {

    NYtBlob::TBlobChunk row;
    row.SetName(filename);

    MD5 md5;
    ui64 offset = 0;
    ui64 chunkSizeLoaded = 0;
    const ui64 chunkSizeToLoad = CHUNK_SIZE_IN_BYTES;
    for (row.MutableChunk()->ReserveAndResize(chunkSizeToLoad);
         chunkSizeLoaded = in.Read(row.MutableChunk()->Detach(), chunkSizeToLoad);
         row.MutableChunk()->ReserveAndResize(chunkSizeToLoad),
         offset += chunkSizeLoaded) {

        if (chunkSizeLoaded < CHUNK_SIZE_IN_BYTES) {
            row.MutableChunk()->ReserveAndResize(chunkSizeLoaded);
        }
        row.SetOffset(offset);

        md5.Update({row.GetChunk().data(), row.GetChunk().size()});

        w->AddRow(row);
    }

    const TString md5Sum = [&md5]{
        char placeholder[33];
        return TString{md5.End(placeholder)};
    }();

    // add special row, containing file metainfo
    row.SetOffset(std::numeric_limits<ui64>::max());
    row.ClearChunk();
    row.MutableInfo()->SetMD5(md5Sum);
    row.MutableInfo()->SetSizeInBytes(offset);
    row.MutableInfo()->SetName(filename);

    w->AddRow(row);
}

void NYtBlob::Upload(
    IInputStream& in, const TString& filename,
    const ::NYT::TYPath& table, NYT::IClientBasePtr client) {

    auto writer = client->CreateTableWriter<TBlobChunk>(
        NYT::TRichYPath{table}
            .Append(true));

    ::Upload(in, filename, writer);

    writer->Finish();
}

void NYtBlob::Download(
    const TString& path, const TStringBuf filename,
    const ::NYT::TYPath& table, ::NYT::IClientBasePtr client,
    const bool storeEntireFileInMemoryWhileDownloading) {

    // TODO: make this operation atomic, e.g. read data to temporary file and then move this
    // temporary file to `path`.

    if (storeEntireFileInMemoryWhileDownloading) {
        TBufferOutput b;
        Download(b, filename, table, client);
        TUnbufferedFileOutput out{path};
        out.Write(b.Buffer().Data(), b.Buffer().Size());
        return;
    }

    TFileOutput out{path};
    Download(out, filename, table, client);
}

void NYtBlob::Download(
    const TString& path, const TStringBuf filename,
    const ::NYT::TYPath& table, ::NYT::IClientBasePtr client) {

    Download(path, filename, table, client, false);
}

void NYtBlob::Download(
    IOutputStream& out, const TStringBuf filename,
    const ::NYT::TYPath& table, ::NYT::IClientBasePtr client) {

    auto reader = client->CreateTableReader<TBlobChunk>(
        NYT::TRichYPath{table}
            .Schema(NYT::CreateTableSchema<TBlobChunk>({"name", "offset"}, false).Strict(true))
            .AddRange(NYT::TReadRange{}.Exact(NYT::TReadLimit{}.Key({TString{filename}}))));

    MD5 md5;
    TBlobInfo info;
    bool fileInfoFound = false;
    ui64 sizeLoaded = 0;
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();

        if (std::numeric_limits<ui64>::max() == row.GetOffset()) {
            // special chunk, containing only file info
            Y_ENSURE(!fileInfoFound);
            fileInfoFound = true;
            info = row.GetInfo();
            continue;
        }

        // chunks must be sequential
        Y_ENSURE(row.GetOffset() == sizeLoaded);

        md5.Update(row.GetChunk());
        out.Write(row.GetChunk());

        sizeLoaded += row.GetChunk().size();
    }

    Y_ENSURE(fileInfoFound);

    const TString md5Sum = [&md5]{
        char placeholder[33];
        return TString{md5.End(placeholder)};
    }();

    Y_ENSURE(info.GetMD5() == md5Sum);
}

bool NYtBlob::TryDownload(
    const TString& path, const TStringBuf filename,
    const ::NYT::TYPath& table, ::NYT::IClientBasePtr client,
    const bool storeEntireFileInMemoryWhileDownloading) {

    try {
        Download(path, filename, table, client, storeEntireFileInMemoryWhileDownloading);
    } catch (std::exception&) {
        if (NFs::Exists(path)) {
            NFs::Remove(path);
        }
        return false;
    }
    return true;
}

bool NYtBlob::TryDownload(
    const TString& path, const TStringBuf filename,
    const ::NYT::TYPath& table, ::NYT::IClientBasePtr client) {

    return TryDownload(path, filename, table, client, false);
}

bool NYtBlob::TryDownload(
    IOutputStream& out, const TStringBuf filename,
    const ::NYT::TYPath& table, ::NYT::IClientBasePtr client) {

    try {
        Download(out, filename, table, client);
    } catch (std::exception&) {
        return false;
    }
    return true;
}

void NYtBlob::Finish(
    const ::NYT::TYPath& table, ::NYT::IClientBasePtr client, const bool uniqueFiles) {

    const auto s = NYT::CreateTableSchema<TBlobChunk>({"name", "offset"}, false)
        .UniqueKeys(uniqueFiles);
    client->Sort(
        NYT::TSortOperationSpec{}
            .AddInput(table)
            .Output(NYT::TRichYPath{table}.Schema(s))
            .SortBy({"name", "offset"}));
}

void NYtBlob::Finish(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client) {
    Finish(table, client, false);
}

void NYtBlob::CreateTable(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client) {
    client->Create(
        table, NYT::NT_TABLE,
        NYT::TCreateOptions{}
            .Recursive(true)
            .Attributes(
                NYT::TNode{}
                    ("_yql_proto_field_info", GenerateProtobufTypeConfig<TBlobInfo>())
                    ("compression_codec", "zstd_5")
                    ("optimize_for", "scan")
                    ("schema", NYT::NodeFromTableSchema(NYT::CreateTableSchema<TBlobChunk>({}, false).Strict(true)))));
}

void NYtBlob::Reset(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client) {
    client->AlterTable(
        table,
        NYT::TAlterTableOptions{}
            .Schema(NYT::CreateTableSchema<TBlobChunk>({}, false).Strict(true)));
}

void NYtBlob::List(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client,
                   TVector<TBlobInfo>& infos) {

    auto reader = client->CreateTableReader<TBlobChunk>(
        ::NYT::TRichYPath{table}
            .Columns({"info"}));

    infos.clear();
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        if (row.HasInfo()) {
            infos.push_back(row.GetInfo());
        }
    }
}

TVector<NYtBlob::TBlobInfo> NYtBlob::List(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client) {
    TVector<TBlobInfo> infos;
    List(table, client, infos);
    return infos;
}
