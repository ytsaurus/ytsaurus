#ifndef YPATH_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_detail.h"
// For the sake of sane code completion.
#include "ypath_detail.h"
#endif

#include "helpers.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute, class TUnderlying>
TYPathBase<Absolute, TUnderlying>::TYPathBase(TStringBuf path)
    : Path_(path)
{
    Validate();
}

template <bool Absolute, class TUnderlying>
TYPathBase<Absolute, TUnderlying>::TYPathBase(const char* path)
    : Path_(path)
{
    Validate();
}

template <bool Absolute, class TUnderlying>
TYPathBase<Absolute, TUnderlying>::TYPathBase(const TString& path)
    : Path_(path)
{
    Validate();
}

template <bool Absolute, class TUnderlying>
TYPathBase<Absolute, TUnderlying>::TYPathBase(const TRawYPath& path)
    : Path_(path.Underlying())
{
    Validate();
}

template <bool Absolute, class TUnderlying>
TString TYPathBase<Absolute, TUnderlying>::GetBaseName() const
{
    if (!Absolute && Path_.empty()) {
        return TString{};
    }
    auto offset = FindLastSegment();
    return NSequoiaClient::ToStringLiteral(TString(Path_.begin() + offset + 1, Path_.end()));
}

template <bool Absolute, class TUnderlying>
TString TYPathBase<Absolute, TUnderlying>::ToString() const
{
    return TString(Path_);
}

template <bool Absolute, class TUnderlying>
TMangledSequoiaPath TYPathBase<Absolute, TUnderlying>::ToMangledSequoiaPath() const
{
    return MangleSequoiaPath(Path_);
}

template <bool Absolute, class TUnderlying>
template <class T>
std::strong_ordering TYPathBase<Absolute, TUnderlying>::operator<=>(const TYPathBase<Absolute, T>& rhs) const noexcept
{
    return Path_.compare(rhs.Underlying()) <=> 0;
}

template <bool Absolute, class TUnderlying>
template <class T>
bool TYPathBase<Absolute, TUnderlying>::operator==(const TYPathBase<Absolute, T>& rhs) const noexcept
{
    return Path_ == rhs.Underlying();
}

template <bool Absolute, class TUnderlying>
TUnderlying& TYPathBase<Absolute, TUnderlying>::Underlying()
{
    return Path_;
}

template <bool Absolute, class TUnderlying>
const TUnderlying& TYPathBase<Absolute, TUnderlying>::Underlying() const
{
    return Path_;
}

inline bool IsForbiddenYPathSymbol(char ch)
{
    return ch == '\0';
}

template <bool Absolute, class TUnderlying>
void TYPathBase<Absolute, TUnderlying>::Validate() const
{
    for (auto ch : Path_) {
        if (IsForbiddenYPathSymbol(ch)) {
            THROW_ERROR_EXCEPTION("Path contains forbidden symbol %Qv", ch);
        }
    }

    if (Absolute) {
        if (Path_.empty()) {
            THROW_ERROR_EXCEPTION("YPath cannot be empty");
        }

        if (!(Path_.StartsWith("//") ||
            Path_.StartsWith(NObjectClient::ObjectIdPathPrefix) ||
            Path_ == "/"))
        {
            THROW_ERROR_EXCEPTION("Path %Qv does not start with a valid root-designator", Path_);
        }
    } else if (!Path_.empty()) {
        NYPath::TTokenizer tokenizer(Path_);
        tokenizer.Advance();
        tokenizer.Skip(NYPath::ETokenType::Ampersand);
        if (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
            tokenizer.Expect(NYPath::ETokenType::Slash);
        }
    }
}

template <bool Absolute, class TUnderlying>
ptrdiff_t TYPathBase<Absolute, TUnderlying>::FindLastSegment() const
{
    auto it = Path_.rbegin();
    bool checkUnescaped = false;
    while (it != Path_.rend()) {
        if (checkUnescaped && *it != '\\') {
            break;
        }
        checkUnescaped = TString(*it) == Separator;
        ++it;
    }
    return Path_.rend() - it;
}

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
bool TYPathBaseImpl<false, TUnderlying>::IsEmpty() const
{
    return TBase::Path_.empty();
}

template <class TUnderlying>
TYPathBuf TYPathBaseImpl<false, TUnderlying>::GetDirPath() const
{
    return TYPathBuf(TStringBuf(TBase::Path_, 0, TBase::FindLastSegment()));
}

template <class TUnderlying>
typename TYPathBaseImpl<false, TUnderlying>::TSegmentView TYPathBaseImpl<false, TUnderlying>::AsSegments() const
{
    return TSegmentView(this);
}

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
TAbsoluteYPathBuf TYPathBaseImpl<true, TUnderlying>::GetDirPath() const
{
    return TAbsoluteYPathBuf(TStringBuf(TBase::Path_, 0, TBase::FindLastSegment()));
}

template <class TUnderlying>
std::pair<TRootDesignator, TYPathBuf>
TYPathBaseImpl<true, TUnderlying>::GetRootDesignator() const
{
    NYPath::TTokenizer tokenizer(TBase::Path_);
    tokenizer.Advance();
    switch (tokenizer.GetType()) {
        case NYPath::ETokenType::Slash:
            return {TSlashRootDesignatorTag{}, TYPathBuf(tokenizer.GetSuffix())};
        case NYPath::ETokenType::Literal: {
            auto token = tokenizer.GetToken();
            if (!token.StartsWith(NObjectClient::ObjectIdPathPrefix)) {
                tokenizer.ThrowUnexpected();
            }

            TStringBuf objectIdString(token.begin() + 1, token.end());
            NCypressClient::TObjectId objectId;
            if (!NCypressClient::TObjectId::FromString(objectIdString, &objectId)) {
                THROW_ERROR_EXCEPTION(
                    NYTree::EErrorCode::ResolveError,
                    "Error parsing object id %v",
                    objectIdString);
            }
            return {objectId, TYPathBuf(tokenizer.GetSuffix())};
        }
        default:
            tokenizer.ThrowUnexpected();
    }
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
typename TYPathBaseImpl<false, TUnderlying>::TSegmentView::const_iterator TYPathBaseImpl<false, TUnderlying>::TSegmentView::begin() const
{
    return TIterator(Owner_, 0);
}

template <class TUnderlying>
typename TYPathBaseImpl<false, TUnderlying>::TSegmentView::const_iterator TYPathBaseImpl<false, TUnderlying>::TSegmentView::end() const
{
    return TIterator(Owner_, Owner_->Path_.size());
}

template <class TUnderlying>
TYPathBaseImpl<false, TUnderlying>::TSegmentView::TSegmentView(const TYPathBaseImpl* owner)
    : Owner_(owner)
{ }

template <class TUnderlying>
bool TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator::operator==(const TIterator& rhs) const
{
    return Owner_ == rhs.Owner_ && Offset_ == rhs.Offset_;
}

template <class TUnderlying>
const TYPathBuf& TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator::operator*() const
{
    return Current_;
}

template <class TUnderlying>
const TYPathBuf* TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator::operator->() const
{
    return &Current_;
}

template <class TUnderlying>
typename TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator& TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator::operator++()
{
    Increment();
    UpdateCurrent();
    return *this;
}

template <class TUnderlying>
typename TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator::operator++(int)
{
    auto result = *this;
    (*this)++;
    return result;
}

template <class TUnderlying>
TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator::TIterator(const TYPathBaseImpl* owner, ptrdiff_t offset)
    : Owner_(owner), Offset_(offset)
{
    auto suffix = TStringBuf(Owner_->Underlying()).Skip(Offset_);
    Tokenizer_.Reset(suffix);
    Tokenizer_.Advance();
    UpdateCurrent();
}

template <class TUnderlying>
void TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator::Increment()
{
    Offset_ += Current_.Underlying().Size();
    if (Tokenizer_.Skip(NYPath::ETokenType::Slash)) {
        ++Offset_;
    }
}

template <class TUnderlying>
void TYPathBaseImpl<false, TUnderlying>::TSegmentView::TIterator::UpdateCurrent()
{
    size_t currentSize = 0;
    bool consumeRootDesignator = Offset_ == 0;
    while ((Tokenizer_.GetType() != NYPath::ETokenType::Slash || consumeRootDesignator) &&
        Tokenizer_.GetType() != NYPath::ETokenType::EndOfStream)
    {
        consumeRootDesignator = false;
        currentSize += Tokenizer_.GetToken().Size();
        Tokenizer_.Advance();
    }
    Current_ = TYPathBuf(Owner_->Path_, Offset_, currentSize);
}

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
template <class T>
TBasicYPathBuf<Absolute>::TBasicYPathBuf(const TYPathBase<Absolute, T>& other)
    : TBase(other.Underlying())
{ }

template <bool Absolute>
template <class T>
TBasicYPathBuf<Absolute>& TBasicYPathBuf<Absolute>::operator=(const TYPathBase<Absolute, T>& rhs)
{
    TBase::Path_ = rhs.Underlying();
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
TBasicYPath<Absolute>::TBasicYPath(const TMangledSequoiaPath& mangledPath)
    : TBase(DemangleSequoiaPath(mangledPath))
{ }

template <bool Absolute>
template <class T>
TBasicYPath<Absolute>::TBasicYPath(const TYPathBase<Absolute, T>& other)
    : TBase(other.Underlying())
{ }

template <bool Absolute>
template <class T>
void TBasicYPath<Absolute>::Join(const TYPathBase<false, T>& other)
{
    TBase::Path_ += other.Underlying();
}

template <bool Absolute>
void TBasicYPath<Absolute>::Append(TString literal)
{
    TBase::Path_ = NYPath::YPathJoin(NYPath::TYPath(TBase::Path_), std::move(literal));
}

template <bool Absolute>
template <class T>
void TBasicYPath<Absolute>::operator+=(const TYPathBase<false, T>& rhs)
{
    Join(rhs);
}

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute, class T, class U>
TBasicYPath<Absolute> YPathJoin(const TYPathBase<Absolute, T>& lhs, const TYPathBase<false, U>& rhs)
{
    TBasicYPath<Absolute> result(lhs);
    result.Join(rhs);
    return result;
}

template <bool Absolute, class T, class U>
TBasicYPath<Absolute> operator+(const TYPathBase<Absolute, T>& lhs, const TYPathBase<false, U>& rhs)
{
    return YPathJoin(lhs, rhs);
}

template <bool Absolute, class T, typename ...TArgs>
TBasicYPath<Absolute> YPathJoin(const TYPathBase<Absolute, T>& path, TArgs&&... literals)
{
    auto joinedPath = NYPath::YPathJoin(NYPath::TYPath(path.Underlying()), literals...);
    return TBasicYPath<Absolute>(joinedPath);
}

////////////////////////////////////////////////////////////////////////////////

template <bool Absolute>
void FormatValue(TStringBuilderBase* builder, const TBasicYPath<Absolute>& path, TStringBuf spec)
{
    FormatValue(builder, path.ToString(), spec);
}

template <bool Absolute>
void FormatValue(TStringBuilderBase* builder, const TBasicYPathBuf<Absolute>& path, TStringBuf spec)
{
    FormatValue(builder, path.ToString(), spec);
}

template <bool Absolute, class TUnderlying>
void Serialize(const TYPathBase<Absolute, TUnderlying>& path, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .Value(path.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

template <bool Absolute>
size_t THash<NYT::NSequoiaClient::TBasicYPath<Absolute>>::operator()(
    const NYT::NSequoiaClient::TBasicYPath<Absolute>& path) const
{
    return ComputeHash(path.Underlying());
}

template <bool Absolute>
size_t THash<NYT::NSequoiaClient::TBasicYPathBuf<Absolute>>::operator()(
    const NYT::NSequoiaClient::TBasicYPathBuf<Absolute>& path) const
{
    return ComputeHash(path.Underlying());
}
