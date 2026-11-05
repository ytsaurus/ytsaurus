#pragma once

#include <cassert>
#include <stdexcept>
#include <string>

/**
 * A lightweight non-owning read-only view into a subsequence of a string.
 */
template <
    typename TChar,
    typename TTraits = std::char_traits<TChar>
>
class
[[deprecated("Obsolete due to C++17's std::string_view. Will be removed in next major release (3.0) ")]]
StringViewImpl {
public:
    using size_type = size_t;
    using traits_type = TTraits;
    using value_type = typename TTraits::char_type;

    static constexpr size_type npos = size_type(-1);

public:
    inline StringViewImpl() noexcept
        : data_(nullptr)
        , size_(0)
    {
    }

    constexpr inline StringViewImpl(const TChar* data, size_t len) noexcept
        : data_(data)
        , size_(len)
    {
    }

    template <size_t len>
    constexpr inline StringViewImpl(const TChar (&str)[len]) noexcept
        : data_(str)
        , size_(len - 1)
    {
    }

    inline StringViewImpl(const TChar* begin, const TChar* end) noexcept
        : data_(begin)
        , size_(end - begin)
    {
        assert(begin <= end);
    }

    inline StringViewImpl(const std::basic_string<TChar>& str) noexcept
        : data_(str.data())
        , size_(str.size())
    {
    }

    inline TChar at(size_type pos) const {
        if (pos >= size_)
            throw std::out_of_range("pos must be less than len");
        return data_[pos];
    }

    inline const TChar* data() const noexcept {
        return data_;
    }

    inline bool empty() const noexcept {
        return size_ == 0;
    }

    inline bool null() const noexcept {
        assert(size_ == 0);
        return data_ == nullptr;
    }

    inline size_type size() const noexcept {
        return size_;
    }

    // to mimic std::string and std::string_view
    inline size_type length() const noexcept {
        return size();
    }

public:
    // Returns a substring [pos, pos + count).
    // If the requested substring extends past the end of the string,
    // or if count == npos, the returned substring is [pos, size()).
    StringViewImpl substr(size_type pos, size_type count = npos) const {
        if (pos >= size_)
            throw std::out_of_range("pos must be less than len");
        if (pos + count >= size_ || count == npos)
            return StringViewImpl(data_ + pos, size_ - pos);
        else
            return StringViewImpl(data_ + pos, count);
    }

    inline const std::basic_string<TChar> to_string() const {
        return std::basic_string<TChar>(data_, size_);
    }

public:
    inline operator bool () const noexcept {
        return !empty();
    }

    inline explicit operator const std::basic_string<TChar> () const {
        return to_string();
    }

    inline TChar operator [] (size_type pos) const noexcept {
        return data_[pos];
    }

    inline bool operator < (const StringViewImpl& other) const noexcept {
        if (size_ < other.size_)
            return true;
        if (size_ > other.size_)
            return false;
        return TTraits::compare(data_, other.data_, size_) < 0;
    }

    inline bool operator == (const StringViewImpl& other) const noexcept {
        if (size_ == other.size_)
            return TTraits::compare(data_, other.data_, size_) == 0;
        return false;
    }

private:
    const TChar* data_;
    size_t size_;
};


// It creates StringView from literal constant at compile time.
template <typename TChar, size_t size>
constexpr inline StringViewImpl<TChar> MakeStringView(const TChar (&str)[size]) {
    return StringViewImpl<TChar>(str, size - 1);
}


using StringView = StringViewImpl<char>;
