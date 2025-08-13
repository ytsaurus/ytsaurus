#include "archive_iterator.h"

#include <sys/types.h>
#include <sys/stat.h>
#include "archive_windows.h"

#include <contrib/libs/libarchive/libarchive/archive.h>
#include <contrib/libs/libarchive/libarchive/archive_entry.h>

#include <util/generic/yexception.h>
#include <util/generic/ptr.h>

using namespace NTar;


class TArchiveIterator::TImpl {
public:
    TImpl(const TString& path);
    ~TImpl();
private:
    void CheckErrno();

    friend class TArchiveIterator;
    TAtomicSharedPtr<TArchiveIterator::TArchiveEntry> CurIterator;
    archive *Archive = nullptr;
    archive_entry *CurEntry = nullptr;

    class TInputFileStream : public IInputStream {
    public:
        TInputFileStream(TImpl *master) : Master(master) {}
    protected:
        size_t DoRead(void* buf, size_t len) final;
    private:
        TImpl *Master = nullptr;
    };
    TInputFileStream CurStream{this};
};

size_t TArchiveIterator::TImpl::TInputFileStream::DoRead(void* buf, size_t len) {
    la_ssize_t read = archive_read_data(Master->Archive, buf, len);
    Master->CheckErrno();
    Y_ENSURE(read >= 0, "Can't read from archive");
    return read;
}

TString TArchiveIterator::TArchiveEntry::GetPath() const {
    EnsureValid();
    return archive_entry_pathname(Master->PImpl->CurEntry);
}

TString TArchiveIterator::TArchiveEntry::GetPathUTF8() const {
    EnsureValid();
    return archive_entry_pathname_utf8(Master->PImpl->CurEntry);
}

TString TArchiveIterator::TArchiveEntry::GetSymLink() const {
    EnsureValid();
    return archive_entry_symlink_utf8(Master->PImpl->CurEntry);
}

bool TArchiveIterator::TArchiveEntry::IsDir() const {
    EnsureValid();
    return S_ISDIR(archive_entry_filetype(Master->PImpl->CurEntry));
}

bool TArchiveIterator::TArchiveEntry::IsRegular() const {
    EnsureValid();
    return S_ISREG(archive_entry_filetype(Master->PImpl->CurEntry));
}

bool TArchiveIterator::TArchiveEntry::IsSymLink() const {
    EnsureValid();
    return S_ISLNK(archive_entry_filetype(Master->PImpl->CurEntry));
}

EArchiveFileType TArchiveIterator::TArchiveEntry::GetType() const {
    EnsureValid();
    if (IsSymLink()) {
        return AFT_SYMLINK;
    } else if (IsDir()) {
        return AFT_DIR;
    } else {
        Y_ABORT_UNLESS(IsRegular());
        return AFT_REGULAR;
    }
}

ui64 TArchiveIterator::TArchiveEntry::GetSize() const {
    EnsureValid();
    Y_ENSURE(archive_entry_size_is_set(Master->PImpl->CurEntry));
    la_int64_t size = archive_entry_size(Master->PImpl->CurEntry);
    Y_ENSURE(size >= 0);
    return size;
}

IInputStream& TArchiveIterator::TArchiveEntry::GetStream() {
    EnsureValid();
    return Master->PImpl->CurStream;
}

void TArchiveIterator::TArchiveEntry::EnsureValid() const {
    Y_ENSURE(Master != nullptr && Master->PImpl->CurIterator.Get() == this, "Invalided archive iterator");
}

TArchiveIterator::TImpl::TImpl(const TString& path) {
    Archive = archive_read_new();
    Y_ENSURE(Archive != nullptr);
    archive_read_support_filter_all(Archive);
    archive_read_support_format_all(Archive);
    Y_ENSURE(archive_read_open_filename(Archive, path.c_str(), 10240) == ARCHIVE_OK, "Can't open archive " << path << ". " << archive_error_string(Archive));
    CheckErrno();
}

TArchiveIterator::TImpl::~TImpl() {
    CheckErrno();
    Y_ENSURE(archive_read_close(Archive) == ARCHIVE_OK);
    CheckErrno();
    archive_read_free(Archive);
}

void TArchiveIterator::TImpl::CheckErrno() {
    if (archive_errno(Archive)) {
        ythrow yexception() << archive_error_string(Archive);
    }
}

TAtomicSharedPtr<TArchiveIterator::TArchiveEntry> TArchiveIterator::Next() {
    int status = archive_read_next_header(PImpl->Archive, &PImpl->CurEntry);
    PImpl->CheckErrno();
    if (status == ARCHIVE_OK) {
        return PImpl->CurIterator = TAtomicSharedPtr<TArchiveIterator::TArchiveEntry>(new TArchiveEntry(this));
    } else {
        Y_ENSURE(status == ARCHIVE_EOF, "Can't read next file from archive");
        PImpl->CurEntry = nullptr;
        return nullptr;
    }
}

TArchiveIterator::TArchiveIterator(const TString& path)
                : PImpl(new TImpl(path)) {}

TArchiveIterator::~TArchiveIterator() {}
