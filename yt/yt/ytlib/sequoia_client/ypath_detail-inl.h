#ifndef YPATH_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_detail.h"
// For the sake of sane code completion.
#include "ypath_detail.h"
#endif

#include "helpers.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute, class TUnderlying>
TString TPathBase<Absolute, TUnderlying>::GetBaseName() const
{
    auto offset = FindLastSegment();
    return NSequoiaClient::ToStringLiteral(TStringBuf(Path_.begin() + offset + 1, Path_.end()));
}

template <bool Absolute, class TUnderlying>
void TPathBase<Absolute, TUnderlying>::RemoveLastSegment()
{
    auto offset = FindLastSegment();
    if constexpr (Absolute) {
        if (offset == 0) {
            return;
        }
    }

    Path_.resize(offset);
}

template <bool Absolute, class TUnderlying>
template <class T>
std::strong_ordering TPathBase<Absolute, TUnderlying>::operator<=>(const TPathBase<Absolute, T>& rhs) const noexcept
{
    return Path_.compare(rhs.Underlying()) <=> 0;
}

template <bool Absolute, class TUnderlying>
template <class T>
bool TPathBase<Absolute, TUnderlying>::operator==(const TPathBase<Absolute, T>& rhs) const noexcept
{
    return Path_ == rhs.Underlying();
}

template <bool Absolute, class TUnderlying>
const TUnderlying& TPathBase<Absolute, TUnderlying>::Underlying() const &
{
    return Path_;
}

template <bool Absolute, class TUnderlying>
TUnderlying&& TPathBase<Absolute, TUnderlying>::Underlying() &&
{
    return std::move(Path_);
}

template <bool Absolute, class TUnderlying>
TPathBase<Absolute, TUnderlying>::TPathBase(const TUnderlying& path)
    : Path_(path)
{ }

template <bool Absolute, class TUnderlying>
TPathBase<Absolute, TUnderlying>::TPathBase(TUnderlying&& path) noexcept
    : Path_(std::move(path))
{ }

template <bool Absolute, class TUnderlying>
ptrdiff_t TPathBase<Absolute, TUnderlying>::FindLastSegment() const
{
    auto it = Path_.rbegin();
    bool checkUnescaped = false;
    while (it != Path_.rend()) {
        if (checkUnescaped && *it != '\\') {
            break;
        }
        checkUnescaped = (*it == Separator);
        ++it;
    }
    return Path_.rend() - it;
}

template <bool Absolute, class TUnderlying>
TUnderlying* TPathBase<Absolute, TUnderlying>::UnsafeMutableUnderlying() noexcept
{
    return &Path_;
}

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
TPathBaseImpl<false, TUnderlying>::TPathBaseImpl()
{ }

template <class TUnderlying>
    bool TPathBaseImpl<false, TUnderlying>::IsEmpty() const
{
    return TBase::Path_.empty();
}

template <class TUnderlying>
TRelativePathBuf TPathBaseImpl<false, TUnderlying>::GetDirPath() const
{
    return TRelativePathBuf::UnsafeMakeCanonicalPath(
        TStringBuf(TBase::Path_, 0, TBase::FindLastSegment()));
}

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
TAbsolutePathBuf TPathBaseImpl<true, TUnderlying>::GetDirPath() const
{
    return TAbsolutePathBuf::UnsafeMakeCanonicalPath(
        TStringBuf(TBase::Path_, 0, TBase::FindLastSegment()));
}

template <class TUnderlying>
TMangledSequoiaPath TPathBaseImpl<true, TUnderlying>::ToMangledSequoiaPath() const
{
    return MangleSequoiaPath(ToRealPath());
}

template <class TUnderlying>
TRealPath TPathBaseImpl<true, TUnderlying>::ToRealPath() const &
{
    return TRealPath(TString(TBase::Underlying()));
}

template <class TUnderlying>
TRealPath TPathBaseImpl<true, TUnderlying>::ToRealPath() &&
{
    return TRealPath(TString(std::move(*this).Underlying()));
}

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
template <class T>
TBasicPathBuf<Absolute>::TBasicPathBuf(const TPathBase<Absolute, T>& other)
    : TBase(TStringBuf(other.Underlying()))
{ }

template <bool Absolute>
template <class T>
TBasicPathBuf<Absolute>& TBasicPathBuf<Absolute>::operator=(const TPathBase<Absolute, T>& rhs)
{
    TBase::Path_ = rhs.Underlying();
    return *this;
}

template <bool Absolute>
TBasicPathBuf<Absolute> TBasicPathBuf<Absolute>::UnsafeMakeCanonicalPath(NYPath::TYPathBuf path)
{
    return TBasicPathBuf(path);
}

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
template <class T>
TBasicPath<Absolute>::TBasicPath(const TPathBase<Absolute, T>& other)
    : TBase(TString(other.Underlying()))
{ }

template <bool Absolute>
template <class T>
void TBasicPath<Absolute>::Join(const TPathBase<false, T>& other)
{
    TBase::Path_ += other.Underlying();
}

template <bool Absolute>
void TBasicPath<Absolute>::Append(TStringBuf literal)
{
    auto key = NYPath::ToYPathLiteral(literal);

    TBase::Path_.reserve(TBase::Path_.size() + 1 + key.size());

    TBase::Path_ += TBase::Separator;
    TBase::Path_ += key;
}

template <bool Absolute>
template <class T>
void TBasicPath<Absolute>::operator+=(const TPathBase<false, T>& rhs)
{
    Join(rhs);
}

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute, class T, class U>
TBasicPath<Absolute> PathJoin(const TPathBase<Absolute, T>& lhs, const TPathBase<false, U>& rhs)
{
    TBasicPath<Absolute> result(lhs);
    result.Join(rhs);
    return result;
}

template <bool Absolute, class T, class U>
TBasicPath<Absolute> operator+(const TPathBase<Absolute, T>& lhs, const TPathBase<false, U>& rhs)
{
    return PathJoin(lhs, rhs);
}

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
void FormatValue(TStringBuilderBase* builder, const TBasicPath<Absolute>& path, TStringBuf spec)
{
    FormatValue(builder, path.Underlying(), spec);
}

template <bool Absolute>
void FormatValue(TStringBuilderBase* builder, const TBasicPathBuf<Absolute>& path, TStringBuf spec)
{
    FormatValue(builder, path.Underlying(), spec);
}

template <bool Absolute, class TUnderlying>
void Serialize(const TPathBase<Absolute, TUnderlying>& path, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .Value(path.Underlying());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

template <bool Absolute>
size_t THash<NYT::NSequoiaClient::TBasicPath<Absolute>>::operator()(
    const NYT::NSequoiaClient::TBasicPath<Absolute>& path) const
{
    return ComputeHash(path.Underlying());
}

template <bool Absolute>
size_t THash<NYT::NSequoiaClient::TBasicPathBuf<Absolute>>::operator()(
    const NYT::NSequoiaClient::TBasicPathBuf<Absolute>& path) const
{
    return ComputeHash(path.Underlying());
}
