#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <yt/yt/core/ypath/token.h>

#include <initializer_list>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCypressPath
{
public:
    TCypressPath() noexcept;

    // Constructors throw exception on invalid path.
    // They have the same check behaviour as Validate.
    TCypressPath(TString path);
    TCypressPath(TStringBuf path);
    TCypressPath(const char* path);
    TCypressPath(std::string path)
        : TCypressPath(TStringBuf(path))
    {
    }

    inline operator const TString&() const noexcept
    {
        return Path_;
    }

    TCypressPath& operator/=(const TCypressPath& other);

    // These versions of /= can be used on paths that don't start with slash
    // but they still check the correctness of the path.
    TCypressPath& operator/=(const TStringBuf& other);
    TCypressPath& operator/=(const TString& other);
    TCypressPath& operator/=(const char* other);

    inline bool operator==(const TCypressPath& that) const noexcept
    {
        return Path_ == that.Path_;
    }

    inline bool operator!=(const TCypressPath& that) const noexcept
    {
        return Path_ != that.Path_;
    }

    // Validate does some basic offline checks of cypress path correctness and throws exception on invalid path.
    // It doesn't check everything but YT server part would still complain about invalid paths.
    static void Validate(TStringBuf path);

    inline const TString& GetPath() const noexcept
    {
        return Path_;
    }

    bool IsAbsolute() const noexcept;

    // Paths that start with single slash are relative.
    // https://wiki.yandex-team.ru/yt/userdoc/ypath/#sintaksisisemantika
    bool IsRelative() const noexcept;

    TCypressPath GetBasename() const noexcept;

    // Returns the whole path if used on empty relative path, cypress root or object guid.
    TCypressPath GetParent() const noexcept;

private:
    struct TStructuredToken
    {
        NYPath::ETokenType Type;
        TStringBuf Value;
    };

    TString Path_;
    TVector<TStructuredToken> Tokens_;

    bool CheckTokenMatch(size_t start, std::initializer_list<NYPath::ETokenType> pattern) const noexcept;

    size_t GetBasenameSize() const noexcept;
};

////////////////////////////////////////////////////////////////////////////////

inline TCypressPath operator/(TCypressPath lhs, const TCypressPath& rhs)
{
    return lhs /= rhs;
}

inline TCypressPath operator/(TCypressPath lhs, const TStringBuf& rhs)
{
    return lhs /= rhs;
}

inline TCypressPath operator/(TCypressPath lhs, const TString& rhs)
{
    return lhs /= rhs;
}

inline TCypressPath operator/(TCypressPath lhs, const std::string& rhs)
{
    return lhs /= TStringBuf(rhs);
}

inline TCypressPath operator/(TCypressPath lhs, const char* rhs)
{
    return lhs /= rhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
