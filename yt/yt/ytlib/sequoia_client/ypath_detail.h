#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute, class TUnderlying>
class TPathBase
{
public:
    //! Returns the path's last segment key.
    TString GetBaseName() const;

    //! Removes the path's last segment. No-op in case of an empty path.
    void RemoveLastSegment();

    //! Compares paths lexicographically according to their canonical representation.
    template <class T>
    std::strong_ordering operator<=>(const TPathBase<Absolute, T>& rhs) const noexcept;
    template <class T>
    bool operator==(const TPathBase<Absolute, T>& rhs) const noexcept;

    const TUnderlying& Underlying() const &;
    TUnderlying&& Underlying() &&;

    static constexpr char Separator = '/';

protected:
    TUnderlying Path_;

    TPathBase(const TUnderlying& path);
    explicit TPathBase(TUnderlying&& path) noexcept;

    ptrdiff_t FindLastSegment() const;
    TUnderlying* UnsafeMutableUnderlying() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute, class TUnderlying>
class TPathBaseImpl;

template <class TUnderlying>
class TPathBaseImpl<false, TUnderlying>
    : public TPathBase<false, TUnderlying>
{
public:
    using TBase = TPathBase<false, TUnderlying>;
    using TBase::TBase;

    TPathBaseImpl();

    [[nodiscard]] bool IsEmpty() const;

    //! Returns the part preceding the base name.
    TRelativePathBuf GetDirPath() const;
};

template <class TUnderlying>
class TPathBaseImpl<true, TUnderlying>
    : public TPathBase<true, TUnderlying>
{
public:
    using TBase = TPathBase<true, TUnderlying>;
    using TBase::TBase;

    //! Returns the part preceding the name.
    TAbsolutePathBuf GetDirPath() const;

    TMangledSequoiaPath ToMangledSequoiaPath() const;

    TRealPath ToRealPath() const &;
    TRealPath ToRealPath() &&;
};

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
class TBasicPathBuf
    : public TPathBaseImpl<Absolute, TStringBuf>
{
public:
    using TBase = TPathBaseImpl<Absolute, TStringBuf>;
    using TBase::TBase;

    template <class T>
    TBasicPathBuf(const TPathBase<Absolute, T>& other);

    template <class T>
    TBasicPathBuf& operator=(const TPathBase<Absolute, T>& rhs);

    //! See #TAbsolutePath::UnsafeMakeCanonicalPath.
    static TBasicPathBuf UnsafeMakeCanonicalPath(NYPath::TYPathBuf path);
};

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
class TBasicPath
    : public TPathBaseImpl<Absolute, TString>
{
public:
    using TBase = TPathBaseImpl<Absolute, TString>;
    using TBase::TBase;

    template <class T>
    TBasicPath(const TPathBase<Absolute, T>& other);

    //! Joins the two paths.
    template <class T>
    void Join(const TPathBase<false, T>& other);

    //! Same as #Join.
    template <class T>
    void operator+=(const TPathBase<false, T>& rhs);

    //! Appends the literal to the path and introduces a directory separator.
    void Append(TStringBuf literal);

    //! Allows to modify the underlying string. Absolutely unsafe since there are no
    //! validations.
    // TODO(kvk1920): provide some sane methods like Rebase() or ReplacePrefix()
    // instead of accessing internal representation directly.
    using TBase::UnsafeMutableUnderlying;
};

////////////////////////////////////////////////////////////////////////////////

class TRelativePath
    : public TBasicPath<false>
{
public:
    using TBase = TBasicPath<false>;
    using TBase::TBase;

    static TRelativePath MakeCanonicalPathOrThrow(NYPath::TYPathBuf path);
    //! See #TAbsolutePath::UnsafeMakeCanonicalPath.
    static TRelativePath UnsafeMakeCanonicalPath(NYPath::TYPath&& path) noexcept;
};

////////////////////////////////////////////////////////////////////////////////

class TAbsolutePath
    : public TBasicPath<true>
{
public:
    using TBase = TBasicPath<true>;
    using TBase::TBase;

    explicit TAbsolutePath(const TMangledSequoiaPath& mangledPath);

    static TAbsolutePath MakeCanonicalPathOrThrow(NYPath::TYPathBuf path);
    //! Construct a path from the canonical path with no validation.
    static TAbsolutePath UnsafeMakeCanonicalPath(NYPath::TYPath&& path) noexcept;
};

////////////////////////////////////////////////////////////////////////////////

//! Joins the two paths.
template <bool Absolute, class T, class U>
TBasicPath<Absolute> PathJoin(const TPathBase<Absolute, T>& lhs, const TPathBase<false, U>& rhs);

//! Same as #PathJoin.
template <bool Absolute, class T, class U>
TBasicPath<Absolute> operator+(const TPathBase<Absolute, T>& lhs, const TPathBase<false, U>& rhs);

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
void FormatValue(TStringBuilderBase* builder, const TBasicPath<Absolute>& path, TStringBuf spec);

template <bool Absolute>
void FormatValue(TStringBuilderBase* builder, const TBasicPathBuf<Absolute>& path, TStringBuf spec);

template <bool Absolute, class TUnderlying>
void Serialize(const TPathBase<Absolute, TUnderlying>& path, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

template <bool Absolute>
struct THash<NYT::NSequoiaClient::TBasicPath<Absolute>>
{
    size_t operator()(const NYT::NSequoiaClient::TBasicPath<Absolute>& path) const;
};

template <bool Absolute>
struct THash<NYT::NSequoiaClient::TBasicPathBuf<Absolute>>
{
    size_t operator()(const NYT::NSequoiaClient::TBasicPathBuf<Absolute>& path) const;
};

template <>
struct THash<NYT::NSequoiaClient::TRelativePath>
    : public THash<NYT::NSequoiaClient::TRelativePath::TBase>
{ };

template <>
struct THash<NYT::NSequoiaClient::TAbsolutePath>
    : public THash<NYT::NSequoiaClient::TAbsolutePath::TBase>
{ };

#define YPATH_DETAIL_INL_H_
#include "ypath_detail-inl.h"
#undef YPATH_DETAIL_INL_H_
