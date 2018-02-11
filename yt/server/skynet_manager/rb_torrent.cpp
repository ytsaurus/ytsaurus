#include "rb_torrent.h"

#include <yt/core/misc/error.h>

#include <yt/core/ytree/fluent.h>

#include <util/stream/str.h>
#include <util/string/iterator.h>

#include <string>

namespace NYT {
namespace NSkynetManager {

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

    virtual void OnStringScalar(const TStringBuf& value) override
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

    virtual void OnKeyedItem(const TStringBuf& name) override
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

    virtual void OnRaw(const TStringBuf& /*yson*/, NYson::EYsonType /*type*/)
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

TString TFileMeta::GetFullSHA1() const
{
    TString fullSHA1;
    TStringOutput out(fullSHA1);

    for (size_t i = 0; i < SHA1.size(); ++i) {
        out.Write(SHA1[i].data(), SHA1[i].size());
    }

    return fullSHA1;
}

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
    const TSkynetShareMeta& meta,
    std::map<TString, TSkynetStructure>* structure,
    std::map<TString, TSkynetTorrent>* torrents)
{
    for (const auto& file : meta.Files) {
        TVector<TString> directories;
        TString filename = file.first;

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

        auto& skynetFile = (*structure)[file.first];
        skynetFile.Type = "file";
        skynetFile.Path = file.first;
        skynetFile.MD5 = file.second.MD5;
        skynetFile.Size = file.second.FileSize;
        skynetFile.Executable = file.second.Executable;

        if (skynetFile.Size == 0) {
            continue;
        }

        TSkynetTorrent torrent;
        torrent.Pieces = file.second.GetFullSHA1();
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

<<<<<<< HEAD
    return TSHA1Hasher().Append(metaBencode.Finish()).GetHexDigestLower();
=======
    return Format("rbtorrent:%v", TSHA1Hasher().Append(metaBencode.Finish()).GetHexDigestLower());
>>>>>>> prestable/19.2
}

TSkynetRbTorrent GenerateResource(const TSkynetShareMeta& meta)
{
    std::map<TString, TSkynetStructure> structure;
    std::map<TString, TSkynetTorrent> torrents;

    CreateTorrentsAndDirs(meta, &structure, &torrents);

    TBencodeWriter head;
    BuildYsonFluently(&head)
        .BeginMap()
            .Item("structure").Value(structure)
            .Item("torrents")
                .DoMapFor(torrents, [&] (
                    TFluentMap fluent,
                    const std::pair<std::string, TSkynetTorrent>& torrent)
                {
                    fluent.Item(torrent.first)
                        .BeginMap()
                            .Item("info").Value(torrent.second)
                        .EndMap();
                })
        .EndMap();
    TString headBinary = head.Finish();

    TSkynetRbTorrent torrent;
    torrent.RbTorrentHash = ComputeRbTorrentId(headBinary);
    torrent.RbTorrentId = "rbtorrent:" + torrent.RbTorrentHash;
    torrent.BencodedTorrentMeta = headBinary;

    return torrent;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
