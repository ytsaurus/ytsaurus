#include "rb_torrent.h"

#include <yt/core/misc/error.h>

#include <yt/core/ytree/fluent.h>

#include <util/stream/str.h>
#include <util/string/iterator.h>

#include <util/string/hex.h>

namespace NYT {
namespace NSkynetManager {

using namespace NCrypto;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

//! Piggybacking on IYsonConsumer interface allows us to use fluent helpers.
class TBencodeWriter
    : public NYson::IYsonConsumer
{
public:
    TBencodeWriter()
        : Out_(Buffer_)
    { }

    virtual void OnStringScalar(TStringBuf value) override
    {
        Out_ << value.Size() << ":" << value;
    }

    virtual void OnInt64Scalar(i64 value) override
    {
        Out_ << "i" << value << "e";
    }

    virtual void OnUint64Scalar(ui64 value) override
    {
        Out_ << "i" << value << "e";
    }

    virtual void OnDoubleScalar(double /*value*/) override
    {
        Y_UNREACHABLE();
    }

    virtual void OnBooleanScalar(bool /*value*/) override
    {
        Y_UNREACHABLE();
    }

    virtual void OnEntity() override
    {
        Y_UNREACHABLE();
    }

    virtual void OnBeginList() override
    {
        Out_.Write('l');
    }

    virtual void OnListItem() override
    { }

    virtual void OnEndList() override
    {
        Out_.Write('e');
    }

    virtual void OnBeginMap() override
    {
        Out_.Write('d');
        LastKeyStack_.emplace_back("");
    }

    virtual void OnKeyedItem(TStringBuf name) override
    {
        if (name < LastKeyStack_.back()) {
            THROW_ERROR_EXCEPTION("Keys in Bencode dictionary are not sorted")
                << TErrorAttribute("key", name)
                << TErrorAttribute("last_key", LastKeyStack_.back());
        }

        LastKeyStack_.back() = name;
        OnStringScalar(name);
    }

    virtual void OnEndMap() override
    {
        Out_.Write('e');
        LastKeyStack_.pop_back();
    }

    virtual void OnBeginAttributes() override
    {
        Y_UNREACHABLE();
    }

    virtual void OnEndAttributes() override
    {
        Y_UNREACHABLE();
    }

    virtual void OnRaw(TStringBuf /*yson*/, NYson::EYsonType /*type*/)
    {
        Y_UNREACHABLE();
    }

    //! Return bencoded representation of value written so far.
    TString Finish()
    {
        return Buffer_;
    }

private:
    TString Buffer_;
    TStringOutput Out_;

    //! Keys of dictionaries in bencode must appear in lexicographical order.
    //!
    //! Client code is responsible for key ordering, consumer
    //! does additional runtime checks.
    //!
    std::vector<TString> LastKeyStack_;
};

////////////////////////////////////////////////////////////////////////////////

struct TSkynetStructure
{
    TString Type;
    TString Path;
    i64 Size;
    bool Executable;

    TMD5Hash MD5;
    TString InfoHash;

    void Save(IYsonConsumer* consumer) const
    {
        //! 'executable', 'md5sum', 'mode', 'path', 'resource', 'size', 'type'
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("executable").Value(Executable ? 1 : 0)
                .DoIf(Type == "file", [this] (TFluentMap fluent) {
                    fluent.Item("md5sum").Value(TStringBuf(MD5.data(), MD5.size()));
                })
                .Item("mode").Value(Executable ? "rwxr-xr-x" : "rw-r--r--")
                .Item("path").Value(Path)
                .Item("resource").Do([this] (auto fluent) {
                    if (Type == "dir") {
                        fluent
                            .BeginMap()
                                .Item("data").Value("mkdir")
                                .Item("id").Value(Format("mkdir:%v", Path))
                                .Item("type").Value("mkdir")
                            .EndMap();
                    } else if (Size == 0) {
                        fluent
                            .BeginMap()
                                .Item("type").Value("touch")
                            .EndMap();
                    } else {
                        fluent
                            .BeginMap()
                                .Item("id").Value(InfoHash)
                                .Item("type").Value("torrent")
                            .EndMap();
                    }
                })
                .Item("size").Value(Size)
                .Item("type").Value(Type)
            .EndMap();
    }
};

void Serialize(const TSkynetStructure& structure, IYsonConsumer* consumer)
{
    structure.Save(consumer);
}

////////////////////////////////////////////////////////////////////////////////

struct TSkynetTorrent
{
    //! Concatenated sha1.
    TString Pieces;
    ui64 Size;
    bool Metadata = false;

    void Save(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("length").Value(Size)
                .Item("name").Value(Metadata ? "metadata" : "data")
                .Item("piece length").Value(SkynetPieceSize)
                .Item("pieces").Value(Pieces)
            .EndMap();
    }
};

void Serialize(const TSkynetTorrent& torrent, IYsonConsumer* consumer)
{
    torrent.Save(consumer);
}

////////////////////////////////////////////////////////////////////////////////

void CreateTorrentsAndDirs(
    const NProto::TResource& meta,
    std::map<TString, TSkynetStructure>* structure,
    std::map<TString, TSkynetTorrent>* torrents)
{
    for (const auto& file : meta.files()) {
        TVector<TString> directories;
        TString filename = file.filename();

        if (filename.find('/') != TString::npos) {
            directories = StringSplitter(filename).Split('/').ToList<TString>();
            filename = directories.back();
            directories.pop_back();
        }

        TString currentDirectory;
        for (const auto& directory : directories) {
            if (currentDirectory.empty()) {
                currentDirectory = directory;
            } else {
                currentDirectory = currentDirectory + "/" + directory;
            }

            if (structure->find(currentDirectory) != structure->end()) {
                continue;
            }

            auto& skynetDirectory = (*structure)[currentDirectory];
            skynetDirectory.Type = "dir";
            skynetDirectory.Path = currentDirectory;
            skynetDirectory.Size = -1;
            skynetDirectory.Executable = true;
        }

        auto& skynetFile = (*structure)[file.filename()];
        skynetFile.Type = "file";
        skynetFile.Path = file.filename();
        skynetFile.MD5 = MD5FromString(file.md5sum());
        skynetFile.Size = file.file_size();
        skynetFile.Executable = false;

        if (skynetFile.Size == 0) {
            continue;
        }

        TSkynetTorrent torrent;
        torrent.Pieces = file.sha1sum();
        torrent.Size = skynetFile.Size;

        TBencodeWriter torrentBencode;
        torrent.Save(&torrentBencode);

        skynetFile.InfoHash = TSHA1Hasher().Append(torrentBencode.Finish()).GetHexDigestLower();

        (*torrents)[skynetFile.InfoHash] = std::move(torrent);
    }
}

TString ComputeRbTorrentId(const TString& headBinary)
{
    TSkynetTorrent metaTorrent;
    metaTorrent.Size = headBinary.Size();
    metaTorrent.Metadata = true;

    TStringOutput piecesOut(metaTorrent.Pieces);

    for (size_t offset = 0; offset < headBinary.Size(); offset += SkynetPieceSize) {
        auto pieceHash = TSHA1Hasher()
                .Append(TStringBuf(headBinary).SubStr(offset, SkynetPieceSize))
                .GetDigest();

        piecesOut.Write(pieceHash.data(), pieceHash.size());
    }

    TBencodeWriter metaBencode;
    Serialize(metaTorrent, &metaBencode);

    return TSHA1Hasher().Append(metaBencode.Finish()).GetHexDigestLower();
}

TResourceDescription ConvertResource(const NProto::TResource& resource, bool needHash, bool needMeta)
{
    std::map<TString, TSkynetStructure> structure;
    std::map<TString, TSkynetTorrent> torrents;

    CreateTorrentsAndDirs(resource, &structure, &torrents);
    auto buildResource = [&] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("structure").Value(structure)
                .Item("torrents")
                    .DoMapFor(torrents, [&] (
                        TFluentMap fluent,
                        const std::pair<TString, TSkynetTorrent>& torrent)
                    {
                        fluent.Item(torrent.first)
                            .BeginMap()
                                .Item("info").Value(torrent.second)
                            .EndMap();
                    })
            .EndMap();
    };
    
    TResourceDescription description;
    if (needHash) {
        TBencodeWriter head;
        buildResource(&head);

        TString headBinary = head.Finish();

        description.ResourceId = ComputeRbTorrentId(headBinary);
    }

    if (needMeta) {
        description.TorrentMeta = BuildYsonNodeFluently().Do([&] (auto value) {
            buildResource(value.GetConsumer());
        });
    }

    return description;
}

////////////////////////////////////////////////////////////////////////////////

bool operator < (const TRowRangeLocation& rhs, const TRowRangeLocation& lhs)
{
    return rhs.RowIndex > lhs.RowIndex;
}

TString MakeFileUrl(
    const TString& node,
    const NChunkClient::TChunkId& chunkId,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 partIndex)
{
    return Format(
        "http://%v/read_skynet_part?chunk_id=%v&lower_row_index=%v&upper_row_index=%v&start_part_index=%v",
        node,
        chunkId,
        lowerRowIndex,
        upperRowIndex,
        partIndex);
}

struct TSkynetLink
{
    i64 Start;
    i64 Size;
    std::vector<TString> Urls;
};

INodePtr MakeLinks(const NProto::TResource& resource, const std::vector<TRowRangeLocation>& locations)
{
    std::map<TString, std::vector<TSkynetLink>> links;

    if (locations.size() == 0) {
        THROW_ERROR_EXCEPTION("Empty locations list");
    }

    for (const auto& file : resource.files()) {
        if (file.file_size() == 0) {
            continue;
        }

        for (i64 partIndex = 0; partIndex < file.row_count(); ) {
            i64 startRow = file.start_row() + partIndex;

            TRowRangeLocation fake;
            fake.RowIndex = startRow;
            auto it = std::lower_bound(locations.rbegin(), locations.rend(), fake);
            if (it == locations.rend()) {
                THROW_ERROR_EXCEPTION("Inconsistent location list")
                    << TErrorAttribute("file", file.filename())
                    << TErrorAttribute("start_row", file.start_row())
                    << TErrorAttribute("part_index", partIndex);
            }

            i64 endRow = std::min(
                file.start_row() + file.row_count(),
                it->RowIndex + it->RowCount + it->LowerLimit.Get(0));
            if (endRow == startRow) {
                THROW_ERROR_EXCEPTION("Inconsistent file")
                    << TErrorAttribute("file", file.filename())
                    << TErrorAttribute("start_row", file.start_row())
                    << TErrorAttribute("row_count", file.row_count());
            }

            i64 fileStart = partIndex * SkynetPieceSize;
            i64 fileEnd = std::min<i64>((endRow - file.start_row()) * SkynetPieceSize, file.file_size());

            TSkynetLink link;
            link.Start = fileStart;
            link.Size = fileEnd - fileStart;

            for (const auto& node : it->Nodes) {
                link.Urls.push_back(MakeFileUrl(
                    node,
                    it->ChunkId,
                    startRow - it->RowIndex,
                    endRow - it->RowIndex,
                    partIndex));
            }

            auto md5 = to_lower(HexEncode(file.md5sum()));
            links[md5].push_back(link);
            partIndex += (endRow - startRow);
        }
    }

    return BuildYsonNodeFluently()
        .DoMapFor(links, [&] (auto fluent, const auto file) {
            fluent
                .Item(file.first)
                .DoListFor(file.second, [&] (auto fluent, const auto link) {
                    fluent.Item()
                        .BeginList()
                            .Item().Value(link.Start)
                            .Item().Value(link.Size)
                            .Item().Value(link.Urls)
                        .EndList();
                });
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
