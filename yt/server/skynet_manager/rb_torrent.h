#pragma once

#include "public.h"

#include <yt/core/crypto/crypto.h>

#include <yt/core/yson/consumer.h>

#include <util/stream/buffer.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

struct TFileMeta
{
    //! MD5 in binary format
    TMD5Hash MD5;

    //! SHA1 of file chunks in binary format
    std::vector<TSHA1Hash> SHA1;

    bool Executable = false;

    ui64 FileSize = 0;

    TString GetFullSHA1() const;
};

struct TSkynetShareMeta
{
    std::map<TString, TFileMeta> Files;
};

struct TSkynetRbTorrent
{
    //! The string user would pass to `sky get` command.
    TString RbTorrentId;

    //! RbTorrentId without prefix.
    TString RbTorrentHash;
    
    //! Bencoded description of this share, we are passing it to skynet daemon.
    TString BencodedTorrentMeta;
};

//! Convert bunch of metadata to resource description.
TSkynetRbTorrent GenerateResource(const TSkynetShareMeta& meta);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
