#pragma once

#include <library/cpp/string_utils/ztstrbuf/ztstrbuf.h>

#include <util/generic/iterator.h>
#include <util/generic/string.h>
#include <util/generic/fwd.h>
#include <util/stream/input.h>

#include <concepts>

struct archive;
struct archive_entry;

namespace NTar {

enum EArchiveFileType {
    AFT_REGULAR,
    AFT_DIR,
    AFT_SYMLINK,
};


class TArchiveIterator : public TInputRangeAdaptor<TArchiveIterator> {
public:
    class TArchiveEntry {
    public:
        TString GetPath() const;
        TString GetPathUTF8() const;
        TString GetSymLink() const;

        bool IsDir() const;
        bool IsRegular() const;
        bool IsSymLink() const;
        bool IsHardlink() const;
        EArchiveFileType GetType() const;

        ui64 GetSize() const;

        IInputStream& GetStream();

    private:
        void EnsureValid() const;

        friend class TArchiveIterator;
        TArchiveEntry(TArchiveIterator *master) : Master(master) {}
        TArchiveIterator *Master = nullptr;
    };

    TArchiveIterator(TZtStringBuf path);

    template <typename T>
    requires std::convertible_to<T, const TString&> && (!std::convertible_to<T, TZtStringBuf>)
    TArchiveIterator(const T& path)  // For backwards compatibility, can be removed after switching from TString to std::string
        : TArchiveIterator(TZtStringBuf(static_cast<const TString&>(path)) )
    {
    }

    ~TArchiveIterator();

private:
    friend class TInputRangeAdaptor<TArchiveIterator>;
    TAtomicSharedPtr<TArchiveEntry> Next();

    class TImpl;
    THolder<TImpl> PImpl;
};

} // namespace NArchive
