#include "helpers.h"

#include <util/string/hex.h>

#include <contrib/libs/openssl/crypto/md5/md5.h>

#include <array>

namespace NYT {
namespace NBlackbox {

////////////////////////////////////////////////////////////////////////////////

TString ComputeMD5(const TString& token)
{
    std::array<char, 16> buffer;
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, token.c_str(), token.length());
    MD5_Final(reinterpret_cast<unsigned char*>(buffer.data()), &ctx);
    return HexEncode(buffer.data(), buffer.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
