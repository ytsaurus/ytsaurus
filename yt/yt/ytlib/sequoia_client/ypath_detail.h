#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct TSlashRootDesignatorTag
{ };

using TRootDesignator = std::variant<NObjectClient::TObjectId, TSlashRootDesignatorTag>;

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute, class TUnderlying>
class TYPathBase
{
public:
    // TODO(kvk1920): add constructors without validation.
    // Rationale: NYPath::TTokenizer operates with raw TStringBuf and can return
    // already validated its suffix. We don't need to run validation twice.

    explicit TYPathBase(TStringBuf path);
    explicit TYPathBase(const char* path);
    explicit TYPathBase(const TString& path);
    explicit TYPathBase(const TRawYPath& path);

    //! Returns the last path segment.
    TString GetBaseName() const;

    void RemoveLastSegment();

    TMangledSequoiaPath ToMangledSequoiaPath() const;

    TRawYPath ToRawYPath() const &;
    TRawYPath ToRawYPath() &&;

    //! Compares paths lexicographically according to their 'String' representation.
    template <class T>
    std::strong_ordering operator<=>(const TYPathBase<Absolute, T>& rhs) const noexcept;
    template <class T>
    bool operator==(const TYPathBase<Absolute, T>& rhs) const noexcept;

    const TUnderlying& Underlying() const &;
    TUnderlying&& Underlying() &&;

    static constexpr char Separator = '/';

protected:
    TUnderlying Path_;

    void Validate() const;
    ptrdiff_t FindLastSegment() const;
    TUnderlying* UnsafeMutableUnderlying() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute, class TUnderlying>
class TYPathBaseImpl;

template <class TUnderlying>
class TYPathBaseImpl<false, TUnderlying>
    : public TYPathBase<false, TUnderlying>
{
public:
    TYPathBaseImpl();

    [[nodiscard]] bool IsEmpty() const;

    using TBase = TYPathBase<false, TUnderlying>;

    using TBase::TBase;

    //! Returns part preceding the name, stripped of trailing slash (if any).
    TYPathBuf GetDirPath() const &;
    TYPathBuf GetDirPath() const && = delete;

    //! Allows iteration over path segments, omitting directory separators.
    class TSegmentView;
    TSegmentView AsSegments() const &;
    TSegmentView AsSegments() const && = delete;

    //! Returns the first path segment, skipping leading separators. Throws when
    //! path doesn't start with separator followed by a literal.
    TStringBuf GetFirstSegment() const &;
    TStringBuf GetFirstSegment() const && = delete;
};

template <class TUnderlying>
class TYPathBaseImpl<true, TUnderlying>
    : public TYPathBase<true, TUnderlying>
{
public:
    using TBase = TYPathBase<true, TUnderlying>;

    using TBase::TBase;

    //! Returns part preceding the name, stripped of trailing slash (if any).
    TAbsoluteYPathBuf GetDirPath() const &;
    TAbsoluteYPathBuf GetDirPath() const && = delete;

    //! Returns the root designator, throws if path does not contain any.
    //! Validates GUID in case of object root designator.
    std::pair<TRootDesignator, TYPathBuf> GetRootDesignator() const &;
    std::pair<TRootDesignator, TYPathBuf> GetRootDesignator() const && = delete;
};

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
class TBasicYPathBuf
    : public TYPathBaseImpl<Absolute, TStringBuf>
{
public:
    using TBase = TYPathBaseImpl<Absolute, TStringBuf>;

    using TBase::TBase;

    template <class T>
    TBasicYPathBuf(const TYPathBase<Absolute, T>& other);

    template <class T>
    TBasicYPathBuf& operator=(const TYPathBase<Absolute, T>& rhs);
};

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
class TBasicYPath
    : public TYPathBaseImpl<Absolute, TString>
{
public:
    using TBase = TYPathBaseImpl<Absolute, TString>;

    using TBase::TBase;

    explicit TBasicYPath(const TMangledSequoiaPath& mangledPath);

    template <class T>
    TBasicYPath(const TYPathBase<Absolute, T>& other);

    //! Joins two paths.
    template <class T>
    void Join(const TYPathBase<false, T>& other);

    //! Same as #Join.
    template <class T>
    void operator+=(const TYPathBase<false, T>& rhs);

    //! Appends literal to the path with introducing a directory separator.
    void Append(TStringBuf literal);

    //! Allows to modify underlying string. Absolutely unsafe since there are no
    //! validations.
    // TODO(kvk1920): provide some sane methods like Rebase() or ReplacePrefix()
    // instead of accessing internal representation directly.
    using TBase::UnsafeMutableUnderlying;
};

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
class TYPathBaseImpl<false, TUnderlying>::TSegmentView
{
public:
    class TIterator;
    using iterator = TIterator;
    using const_iterator = iterator;

    const_iterator begin() const;
    const_iterator end() const;

private:
    const TYPathBaseImpl* Owner_;

    friend class TYPathBaseImpl;

    explicit TSegmentView(const TYPathBaseImpl* owner);
};

template <class TUnderlying>
class TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator
{
public:
    bool operator==(const TIterator& rhs) const;

    const TYPathBuf& operator*() const;
    const TYPathBuf* operator->() const;

    TIterator& operator++();
    TIterator operator++(int);

private:
    const TYPathBaseImpl* Owner_ = nullptr;
    ptrdiff_t Offset_ = 0;
    TYPathBuf Current_;

    friend class TSegmentView;

    TIterator(const TYPathBaseImpl* owner, ptrdiff_t offset);

    NYPath::TTokenizer GetTokenizer() const;

    void Increment();
    void UpdateCurrent();
};

////////////////////////////////////////////////////////////////////////////////

//! Joins two YPaths.
template <bool Absolute, class T, class U>
TBasicYPath<Absolute> YPathJoin(const TYPathBase<Absolute, T>& lhs, const TYPathBase<false, U>& rhs);

//! Same as #YPathJoin.
template <bool Absolute, class T, class U>
TBasicYPath<Absolute> operator+(const TYPathBase<Absolute, T>& lhs, const TYPathBase<false, U>& rhs);

//! Appends literals to the path with introducing a directory separator.
template <bool Absolute, class T, typename ...TArgs>
TBasicYPath<Absolute> YPathJoin(const TYPathBase<Absolute, T>& path, TArgs&&... literals);

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
void FormatValue(TStringBuilderBase* builder, const TBasicYPath<Absolute>& path, TStringBuf spec);

template <bool Absolute>
void FormatValue(TStringBuilderBase* builder, const TBasicYPathBuf<Absolute>& path, TStringBuf spec);

template <bool Absolute, class TUnderlying>
void Serialize(const TYPathBase<Absolute, TUnderlying>& path, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

template <bool Absolute>
struct THash<NYT::NSequoiaClient::TBasicYPath<Absolute>>
{
    size_t operator()(const NYT::NSequoiaClient::TBasicYPath<Absolute>& path) const;
};

template <bool Absolute>
struct THash<NYT::NSequoiaClient::TBasicYPathBuf<Absolute>>
{
    size_t operator()(const NYT::NSequoiaClient::TBasicYPathBuf<Absolute>& path) const;
};

#define YPATH_DETAIL_INL_H_
#include "ypath_detail-inl.h"
#undef YPATH_DETAIL_INL_H_
