#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct TSlashRootDesignatorTag
{ };

template <class TUnderlying>
class TYPathBase
{
public:
    TYPathBase() = default;
    explicit TYPathBase(TStringBuf path);
    explicit TYPathBase(const char* path);
    explicit TYPathBase(const TString& path);

    //! Returns the last path segment.
    TYPathBuf GetBaseName() const;
    //! Returns part preceding the name, stripped of trailing slash (if any).
    TYPathBuf GetDirPath() const;

    //! Returns the root designator, or 'std::monostate' if path does not contain any.
    //! Validates GUID in case of object root designator.
    std::variant<std::monostate, TGuid, TSlashRootDesignatorTag> GetRootDesignator() const;

    [[nodiscard]] bool Empty() const;

    //! Returns formatted path with escaping of special characters.
    TString ToString() const;
    TMangledSequoiaPath ToMangledSequoiaPath() const;
    //! Returns formatted path without escaping of special characters.
    TString ToStringLiteral() const;

    //! Compares paths lexicographically according to their 'String' representation.
    template <class T>
    std::strong_ordering operator<=>(const TYPathBase<T>& rhs) const noexcept;
    template <class T>
    bool operator==(const TYPathBase<T>& rhs) const noexcept;

    //! Allows iteration over path segments, omitting directory separators.
    class TSegmentView;
    TSegmentView AsSegments() const;

    TUnderlying& Underlying();
    const TUnderlying& Underlying() const;

    static constexpr TStringBuf Separator = "/";

protected:
    TUnderlying Path_;

    void Validate() const;
    ptrdiff_t FindLastSegment() const;
};

////////////////////////////////////////////////////////////////////////////////

class TYPathBuf
    : public TYPathBase<TStringBuf>
{
public:
    using TBase = TYPathBase<TStringBuf>;

    using TBase::TBase;

    template <class T>
    TYPathBuf(const TYPathBase<T>& other);

    template <class T>
    TYPathBuf& operator=(const TYPathBase<T>& rhs);
};

////////////////////////////////////////////////////////////////////////////////

class TYPath
    : public TYPathBase<TString>
{
public:
    using TBase = TYPathBase<TString>;

    using TBase::TBase;

    explicit TYPath(const TMangledSequoiaPath& mangledPath);

    template <class T>
    explicit TYPath(const TYPathBase<T>& other);

    template <class T>
    TYPath& operator=(const TYPathBase<T>& rhs);

    //! Appends elements to the path with a directory separator.
    template <class T>
    TYPath& Append(const TYPathBase<T>& other);

    //! Concatenates two paths without introducing a directory separator.
    template <class T>
    TYPath& Concat(const TYPathBase<T>& other);

    //! Same as #Append.
    template <class T>
    TYPath& operator/=(const TYPathBase<T>& rhs);

    //! Same as #Concat.
    template <class T>
    TYPath& operator+=(const TYPathBase<T>& rhs);
};

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
class TYPathBase<TUnderlying>::TSegmentView
{
public:
    class TIterator;
    using iterator = TIterator;
    using const_iterator = iterator;

    // TODO(danilalexeev): Support backward iteration.
    const_iterator begin() const;
    const_iterator end() const;

private:
    const TYPathBase* Owner_;

    friend class TYPathBase;

    explicit TSegmentView(const TYPathBase* owner);
};

template <class TUnderlying>
class TYPathBase<TUnderlying>::TSegmentView::TIterator
{
public:
    bool operator==(const TIterator& rhs) const;

    const TYPathBuf& operator*() const;
    const TYPathBuf* operator->() const;

    TIterator& operator++();
    TIterator operator++(int);

private:
    const TYPathBase* Owner_ = nullptr;
    ptrdiff_t Offset_ = 0;
    NYPath::TTokenizer Tokenizer_;
    TYPathBuf Current_;

    friend class TSegmentView;

    TIterator(const TYPathBase* owner, ptrdiff_t offset);

    void Increment();
    void UpdateCurrent();
};

////////////////////////////////////////////////////////////////////////////////

//! Concatenates two paths with a directory separator.
template <class T, class U>
TYPath MergePaths(const TYPathBase<T>& lhs, const TYPathBase<U>& rhs);

//! Concatenates two paths without introducing a directory separator.
template <class T, class U>
TYPath ConcatPaths(const TYPathBase<T>& lhs, const TYPathBase<U>& rhs);

//! Same as #MergePaths.
template <class T, class U>
TYPath operator/(const TYPathBase<T>& lhs, const TYPathBase<U>& rhs);

//! Same as #ConcatPaths.
template <class T, class U>
TYPath operator+(const TYPathBase<T>& lhs, const TYPathBase<U>& rhs);

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TYPath& path);
TString ToString(TYPathBuf path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

template <class TUnderlying>
struct THash<NYT::NSequoiaClient::TYPathBase<TUnderlying>>
{
    size_t operator()(const NYT::NSequoiaClient::TYPathBase<TUnderlying>& path) const;
};

#define YPATH_DETAIL_INL_H_
#include "ypath_detail-inl.h"
#undef YPATH_DETAIL_INL_H_
