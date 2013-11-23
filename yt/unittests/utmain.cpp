#include "stdafx.h"

#include <ytlib/shutdown.h>

#include <server/hydra/file_changelog.h>

#include <util/datetime/base.h>

#include <util/random/random.h>

#include <util/string/printf.h>
#include <util/string/escape.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka GenerateRandomFileName(const char* prefix)
{
    return Sprintf("%s-%016" PRIx64 "-%016" PRIx64,
            prefix,
            MicroSeconds(),
            RandomNumber<ui64>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace testing {

////////////////////////////////////////////////////////////////////////////////

Matcher<const TStringBuf&>::Matcher(const Stroka& s)
{
    *this = Eq(TStringBuf(s));
}

Matcher<const TStringBuf&>::Matcher(const char* s)
{
    *this = Eq(TStringBuf(s));
}

Matcher<const TStringBuf&>::Matcher(const TStringBuf& s)
{
    *this = Eq(s);
}

Matcher<const Stroka&>::Matcher(const Stroka& s)
{
    *this = Eq(s);
}

Matcher<const Stroka&>::Matcher(const char* s)
{
    *this = Eq(Stroka(s));
}

Matcher<Stroka>::Matcher(const Stroka& s)
{
    *this = Eq(s);
}

Matcher<Stroka>::Matcher(const char* s)
{
    *this = Eq(Stroka(s));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace testing

void PrintTo(const Stroka& string, ::std::ostream* os)
{
    *os << string.c_str();
}

void PrintTo(const TStringBuf& string, ::std::ostream* os)
{
    *os << Stroka(string);
}

int main(int argc, char **argv)
{
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif

    testing::InitGoogleTest(&argc, argv);
    int rv = RUN_ALL_TESTS();

    NYT::NHydra::ShutdownChangelogs();
    NYT::Shutdown();

    return rv;
}
