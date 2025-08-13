#include "archive_writer.h"

#include <contrib/libs/libarchive/libarchive/archive.h>
#include <contrib/libs/libarchive/libarchive/archive_entry.h>

#include "archive_windows.h"

#include <util/string/cast.h>
#include <util/memory/blob.h>

using namespace NTar;

class TArchiveWriter::TImpl {
public:
    TImpl(const TString& path,
          EArchiveType type = AT_FROM_EXTENSION_OR_TAR,
          EArchiveCompression compression = AC_FROM_EXTENSION_OR_NONE,
          EArchiveEncoding encoding = AE_NONE);
    ~TImpl();

    void WriteDir(const TString& path);
    void WriteFile(const TString& path, TBlob& blob);
    void WriteFileFrom(const TString& path, ui64 size, IInputStream& stream);
    void WriteSymlink(const TFsPath& path, const TFsPath& target);
    IOutputStream& GetStream(ui64 size);
private:
    void WriteEntry(const TString& path, int type, const TMaybe<ui64>& fileSize = {},
                    const TMaybe<TString>& linkTarget = {});
    bool IsWriting();
    void EnsureNotWriting();
    void CheckErrno();
    void CheckResult(int result);

    class TOutputFileStream : public IOutputStream {
    public:
        TOutputFileStream(TArchiveWriter::TImpl *master, size_t bytesLeft) : Master(master), BytesLeft(bytesLeft) {}
    protected:
        void DoWrite(const void* buf, size_t len) final;
    private:
        friend TArchiveWriter::TImpl;
        TArchiveWriter::TImpl *Master = nullptr;
        size_t BytesLeft = 0;
    };

    archive *Archive = nullptr;
    TMaybe<TOutputFileStream> CurStream;
};

TArchiveWriter::TImpl::TImpl(const TString& path,
                             EArchiveType type,
                             EArchiveCompression compression,
                             EArchiveEncoding encoding)
{
    Archive = archive_write_new();
    Y_ENSURE(Archive != nullptr);
    CheckErrno();

    if (type == AT_FROM_EXTENSION) {
        CheckResult(archive_write_set_format_filter_by_ext(Archive, path.c_str()));
    } else if (type == AT_FROM_EXTENSION_OR_TAR) {
        CheckResult(archive_write_set_format_filter_by_ext_def(Archive, path.c_str(), ".tar"));
    } else {
        TString name = ToString(type);
        CheckResult(archive_write_set_format_by_name(Archive, name.c_str()));
    }

    if (compression != AC_NONE) {
        if (compression == AC_FROM_EXTENSION_OR_NONE) {
            Y_ENSURE(type == AT_FROM_EXTENSION || type == AT_FROM_EXTENSION_OR_TAR);
        } else {
            TString name = ToString(compression);
            CheckResult(archive_write_add_filter_by_name(Archive, name.c_str()));
        }
    }

    if (encoding != AE_NONE) {
        TString name = ToString(encoding);
        CheckResult(archive_write_add_filter_by_name(Archive, name.c_str()));
    }

    CheckResult(archive_write_open_filename(Archive, path.c_str()));
}

TArchiveWriter::TImpl::~TImpl() {
    EnsureNotWriting();
    CheckErrno();
    CheckResult(archive_write_close(Archive));
    archive_write_free(Archive);
}

void TArchiveWriter::TImpl::WriteEntry(const TString& path, int type, const TMaybe<ui64>& fileSize,
                                       const TMaybe<TString>& linkTarget) {
    EnsureNotWriting();
    archive_entry *entry = archive_entry_new2(Archive);
    CheckErrno();
    Y_ENSURE(entry != nullptr);
    archive_entry_set_pathname(entry, path.c_str());
    if (fileSize.Defined()) {
        archive_entry_set_size(entry, fileSize.GetRef());
    }
    if (linkTarget.Defined()) {
        archive_entry_set_symlink_utf8(entry, linkTarget.GetRef().c_str());
    }
    archive_entry_set_filetype(entry, type);
    archive_entry_set_perm(entry, 0766);
    CheckResult(archive_write_header(Archive, entry));
    archive_entry_free(entry);
}

void TArchiveWriter::TImpl::WriteDir(const TString& path) {
    WriteEntry(path, S_IFDIR);
}

void TArchiveWriter::TImpl::WriteFile(const TString& path, TBlob& blob) {
    WriteEntry(path, S_IFREG, blob.Size());
    GetStream(blob.Size()).Write(blob.Data(), blob.Size());
}

void TArchiveWriter::TImpl::WriteFileFrom(const TString& path, ui64 size, IInputStream& stream) {
    WriteEntry(path, S_IFREG, size);
    stream.ReadAll(GetStream(size));
    EnsureNotWriting();
}

void TArchiveWriter::TImpl::WriteSymlink(const TFsPath &path, const TFsPath &target) {
    WriteEntry(path, S_IFLNK, {}, target);
}

IOutputStream& TArchiveWriter::TImpl::GetStream(ui64 size) {
    if (!IsWriting()) {
        CurStream = TOutputFileStream(this, size);
    }
    return CurStream.GetRef();
}

bool TArchiveWriter::TImpl::IsWriting() {
    if (CurStream.Defined() && CurStream.GetRef().BytesLeft == 0) {
        CurStream = {};
    }
    return CurStream.Defined();
}

void TArchiveWriter::TImpl::EnsureNotWriting() {
    Y_ENSURE(!IsWriting(), "Can't modify entry since data writting started");
}

void TArchiveWriter::TImpl::CheckErrno() {
    if (archive_errno(Archive)) {
        TString error = archive_error_string(Archive);
        if (error == "Write error") {
            error += ". Probably no space left on device";
        }
        ythrow yexception() << error;

    }
}

void TArchiveWriter::TImpl::CheckResult(int res) {
    CheckErrno();
    Y_ENSURE(res == ARCHIVE_OK);
}

void TArchiveWriter::TImpl::TOutputFileStream::DoWrite(const void* buf, size_t len) {
    Y_ENSURE(BytesLeft >= len);
    BytesLeft -= len;
    while (len > 0) {
        la_ssize_t written = archive_write_data(Master->Archive, buf, len);
        Master->CheckErrno();
        Y_ENSURE(written > 0);
        len -= written;
    }
}

TArchiveWriter::TArchiveWriter(const TString& path,
                               EArchiveType type,
                               EArchiveCompression compression,
                               EArchiveEncoding encoding)
                               : PImpl(new TImpl(path, type, compression, encoding)) {}

TArchiveWriter::~TArchiveWriter() {}

void TArchiveWriter::WriteDir(const TString& path) {
    PImpl->WriteDir(path);
}

void TArchiveWriter::WriteFile(const TString& path, TBlob& blob) {
    PImpl->WriteFile(path, blob);
}

void TArchiveWriter::WriteFileFrom(const TString& path, ui64 size, IInputStream& stream) {
    PImpl->WriteFileFrom(path, size, stream);
}

void TArchiveWriter::WriteSymlink(const TFsPath& path, const TFsPath& target) {
    PImpl->WriteSymlink(path, target);
}
