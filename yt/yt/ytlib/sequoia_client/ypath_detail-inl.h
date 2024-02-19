#ifndef YPATH_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_detail.h"
// For the sake of sane code completion.
#include "ypath_detail.h"
#endif

#include "helpers.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
TYPathBase<TUnderlying>::TYPathBase(TStringBuf path)
    : Path_(path)
{
    Validate();
}

template <class TUnderlying>
TYPathBase<TUnderlying>::TYPathBase(const char* path)
    : Path_(path)
{
    Validate();
}

template <class TUnderlying>
TYPathBase<TUnderlying>::TYPathBase(const TString& path)
    : Path_(path)
{
    Validate();
}

template <class TUnderlying>
TYPathBuf TYPathBase<TUnderlying>::GetBaseName() const
{
    auto offset = FindLastSegment();
    return TYPathBuf(TStringBuf(Path_, offset > 0 ? offset + 1 : offset));
}

template <class TUnderlying>
TYPathBuf TYPathBase<TUnderlying>::GetDirPath() const
{
    return TYPathBuf(TStringBuf(Path_, 0, FindLastSegment()));
}

template <class TUnderlying>
std::variant<std::monostate, TGuid, TSlashRootDesignatorTag>
TYPathBase<TUnderlying>::GetRootDesignator() const
{
    NYPath::TTokenizer tokenizer(Path_);
    tokenizer.Advance();
    switch (tokenizer.GetType()) {
        case NYPath::ETokenType::Slash:
            return TSlashRootDesignatorTag{};
        case NYPath::ETokenType::Literal: {
            auto token = tokenizer.GetToken();
            if (!token.StartsWith(NObjectClient::ObjectIdPathPrefix)) {
                return {};
            }

            TStringBuf objectIdString(token.begin() + 1, token.end());
            NCypressClient::TObjectId objectId;
            if (!NCypressClient::TObjectId::FromString(objectIdString, &objectId)) {
                THROW_ERROR_EXCEPTION(
                    NYTree::EErrorCode::ResolveError,
                    "Error parsing object id %v",
                    objectIdString);
            }
            return objectId;
        }
        default:
            break;
    }
    return {};
}

template <class TUnderlying>
bool TYPathBase<TUnderlying>::Empty() const
{
    return Path_.empty();
}

template <class TUnderlying>
TString TYPathBase<TUnderlying>::ToString() const
{
    return TString(Path_);
}

template <class TUnderlying>
TMangledSequoiaPath TYPathBase<TUnderlying>::ToMangledSequoiaPath() const
{
    return MangleSequoiaPath(Path_);
}

template <class TUnderlying>
TString TYPathBase<TUnderlying>::ToStringLiteral() const
{
    TStringBuilder builder;
    for (NYPath::TTokenizer tokenizer(Path_); tokenizer.GetType() != NYPath::ETokenType::EndOfStream; tokenizer.Advance()) {
        builder.AppendString(tokenizer.GetType() == NYPath::ETokenType::Literal
            ? tokenizer.GetLiteralValue()
            : TString(tokenizer.GetToken()));
    }
    return builder.Flush();
}

template <class TUnderlying>
template <class T>
std::strong_ordering TYPathBase<TUnderlying>::operator<=>(const TYPathBase<T>& rhs) const noexcept
{
    return Path_.compare(rhs.Underlying()) <=> 0;
}

template <class TUnderlying>
template <class T>
bool TYPathBase<TUnderlying>::operator==(const TYPathBase<T>& rhs) const noexcept
{
    return Path_ == rhs.Underlying();
}

template <class TUnderlying>
TUnderlying& TYPathBase<TUnderlying>::Underlying()
{
    return Path_;
}

template <class TUnderlying>
const TUnderlying& TYPathBase<TUnderlying>::Underlying() const
{
    return Path_;
}

inline bool IsForbiddenYPathSymbol(char ch)
{
    return ch == '\0';
}

template <class TUnderlying>
void TYPathBase<TUnderlying>::Validate() const
{
    if (Path_.StartsWith("/") && !Path_.StartsWith("//")) {
        THROW_ERROR_EXCEPTION("Path begins with \"/\" but not with \"//\"");
    }
    for (auto ch : Path_) {
        if (IsForbiddenYPathSymbol(ch)) {
            THROW_ERROR_EXCEPTION("Path contains forbidden symbol %Qv",
                ch);
        }
    }
}

template <class TUnderlying>
ptrdiff_t TYPathBase<TUnderlying>::FindLastSegment() const
{
    auto it = Path_.rbegin();
    bool checkUnescaped = false;
    while (it != Path_.rend()) {
        if (checkUnescaped && *it != '\\') {
            break;
        }
        checkUnescaped = *it == Separator;
    }
    return Path_.rend() - it;
}

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
typename TYPathBase<TUnderlying>::TSegmentView::const_iterator TYPathBase<TUnderlying>::TSegmentView::begin() const
{
    return TIterator(Owner_, 0);
}

template <class TUnderlying>
typename TYPathBase<TUnderlying>::TSegmentView::const_iterator TYPathBase<TUnderlying>::TSegmentView::end() const
{
    return TIterator(Owner_, Owner_->Path_.size());
}

template <class TUnderlying>
TYPathBase<TUnderlying>::TSegmentView::TSegmentView(const TYPathBase* owner)
    : Owner_(owner)
{ }

template <class TUnderlying>
bool TYPathBase<TUnderlying>::TSegmentView::TIterator::operator==(const TIterator& rhs) const
{
    return Owner_ == rhs.Owner_ && Offset_ == rhs.Offset_;
}

template <class TUnderlying>
const TYPathBuf& TYPathBase<TUnderlying>::TSegmentView::TIterator::operator*() const
{
    return Current_;
}

template <class TUnderlying>
const TYPathBuf* TYPathBase<TUnderlying>::TSegmentView::TIterator::operator->() const
{
    return &Current_;
}

template <class TUnderlying>
typename TYPathBase<TUnderlying>::TSegmentView::TIterator& TYPathBase<TUnderlying>::TSegmentView::TIterator::operator++()
{
    Increment();
    UpdateCurrent();
    return *this;
}

template <class TUnderlying>
typename TYPathBase<TUnderlying>::TSegmentView::TIterator TYPathBase<TUnderlying>::TSegmentView::TIterator::operator++(int)
{
    auto result = *this;
    (*this)++;
    return result;
}

template <class TUnderlying>
TYPathBase<TUnderlying>::TSegmentView::TIterator::TIterator(const TYPathBase* owner, ptrdiff_t offset)
    : Owner_(owner), Offset_(offset)
{
    auto suffix = TStringBuf(Owner_->Underlying()).Skip(Offset_);
    Tokenizer_.Reset(suffix);
    Tokenizer_.Advance();
    UpdateCurrent();
}

template <class TUnderlying>
void TYPathBase<TUnderlying>::TSegmentView::TIterator::Increment()
{
    Offset_ += Current_.Underlying().Size();
    if (Tokenizer_.Skip(NYPath::ETokenType::Slash)) {
        ++Offset_;
    }
}

template <class TUnderlying>
void TYPathBase<TUnderlying>::TSegmentView::TIterator::UpdateCurrent()
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

template <class TUnderlying>
typename TYPathBase<TUnderlying>::TSegmentView TYPathBase<TUnderlying>::AsSegments() const
{
    return TSegmentView(this);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYPathBuf::TYPathBuf(const TYPathBase<T>& other)
    : TBase(other.Underlying())
{ }

template <class T>
TYPathBuf& TYPathBuf::operator=(const TYPathBase<T>& rhs)
{
    Path_ = rhs.Underlying();
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYPath::TYPath(const TYPathBase<T>& other)
    : TBase(other.Underlying())
{ }

template <class T>
TYPath& TYPath::operator=(const TYPathBase<T>& rhs)
{
    Path_ = rhs.Underlying();
    return *this;
}

template <class T>
TYPath& TYPath::Append(const TYPathBase<T>& other)
{
    if (!std::holds_alternative<std::monostate>(GetRootDesignator())) {
        THROW_ERROR_EXCEPTION("Appending path with root is invalid");
    }
    Path_ += Separator;
    Path_ += other.Underlying();
    return *this;
}

template <class T>
TYPath& TYPath::Concat(const TYPathBase<T>& other)
{
    if (!std::holds_alternative<std::monostate>(GetRootDesignator())) {
        THROW_ERROR_EXCEPTION("Concatenating path with root is invalid");
    }
    Path_ += other.Underlying();
    return *this;
}

template <class T>
TYPath& TYPath::operator/=(const TYPathBase<T>& rhs)
{
    return Append(rhs);
}

template <class T>
TYPath& TYPath::operator+=(const TYPathBase<T>& rhs)
{
    return Concat(rhs);
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class U>
TYPath MergePaths(const TYPathBase<T>& lhs, const TYPathBase<U>& rhs)
{
    TYPath result{lhs};
    result.Append(rhs);
    return result;
}

template <class T, class U>
TYPath ConcatPaths(const TYPathBase<T>& lhs, const TYPathBase<U>& rhs)
{
    TYPath result{lhs};
    result.Concat(rhs);
    return result;
}

template <class T, class U>
TYPath operator/(const TYPathBase<T>& lhs, const TYPathBase<U>& rhs)
{
    return MergePaths(lhs, rhs);
}

template <class T, class U>
TYPath operator+(const TYPathBase<T>& lhs, const TYPathBase<U>& rhs)
{
    return ConcatPaths(lhs, rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

template <class TUnderlying>
size_t THash<NYT::NSequoiaClient::TYPathBase<TUnderlying>>::operator()(const NYT::NSequoiaClient::TYPathBase<TUnderlying>& path) const
{
    return ComputeHash(path.Underlying());
}
