#pragma once

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/fwd.h>
#include <util/generic/maybe.h>

class TBlob;

namespace NTar {

enum EArchiveType {
    AT_FROM_EXTENSION /* "from_ext" */,
    AT_FROM_EXTENSION_OR_TAR /* "auto" */,
    AT_7ZIP /* "7zip" */,
    AT_AR /* "ar" */,
    AT_AR_BSD /* "arbsd" */,
    AT_AR_SVR4 /* "argnu" */,
    AT_CPIO /* "odc" */,
    AT_CPIO_NEWC /* "newc" */,
    AT_GNUTAR /* "gnutar" */,
    AT_ISO9660 /* "iso9660" */,
    AT_MTREE /* "mtree" */,
    AT_MTREE_CLASSIC /* "mtree-classic" */,
    AT_PAX /* "posix" */,
    AT_PAX_RESTRICTED /* "bsdtar" */, // default for ".tar"
    AT_RAW /* "raw" */,
    AT_SHAR /* "shar" */,
    AT_SHAR_DUMP /* "shardump" */,
    AT_USTAR /* "ustar" */,
    AT_V7TAR /* "oldtar" */,
    AT_WARC /* "warc" */,
    AT_XAR /* "xar" */,
    AT_ZIP /* "zip" */,
};

enum EArchiveCompression {
    AC_FROM_EXTENSION_OR_NONE /* "auto" */, // available only if type is from ext or auto
    AC_NONE /* "none" */,
    AC_BZIP2 /* "bzip2" */,
    AC_COMPRESS /* "compress" */,
    AC_GRZIP /* "grzip" */,
    AC_GZIP /* "gzip" */,
    AC_LRZIP /* "lrzip" */,
    AC_LZ4 /* "lz4" */,
    AC_LZIP /* "lzip" */,
    AC_LZMA /* "lzma" */,
    AC_LZOP /* "lzop" */,
    AC_XZ /* "xz" */,
    AC_ZSTD /* "zstd" */,
};

enum EArchiveEncoding {
    AE_NONE /* "none" */,
    AE_BASE64 /* "b64encode" */,
    AE_UUE /* "uuencode" */,
};

class TArchiveWriter {
public:
    TArchiveWriter(const TString& path,
                   EArchiveType type = AT_FROM_EXTENSION_OR_TAR,
                   EArchiveCompression compression = AC_FROM_EXTENSION_OR_NONE,
                   EArchiveEncoding encoding = AE_NONE);
    ~TArchiveWriter();

    void WriteDir(const TString& path);
    void WriteFile(const TString& path, TBlob& blob);
    void WriteFileFrom(const TString& path, ui64 size, IInputStream& stream);
    void WriteSymlink(const TFsPath& path, const TFsPath& target);

private:
    class TImpl;
    THolder<TImpl> PImpl;
};


} // namespace NTar
