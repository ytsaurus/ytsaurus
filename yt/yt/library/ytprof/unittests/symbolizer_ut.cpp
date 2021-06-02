#include <gtest/gtest.h>

#include <yt/yt/library/ytprof/symbolize.h>

namespace NYT::NProf {

Y_NO_INLINE void* GetIP()
{
    return __builtin_return_address(0);
}

TEST(Symbolize, EmptyProfile)
{
    Profile profile;
    profile.add_string_table();

    Symbolize(&profile);
}

TEST(Symbolize, SingleLocation)
{
    Profile profile;
    profile.add_string_table();

    auto thisIP = GetIP();

    {
        auto location = profile.add_location();
        location->set_address(reinterpret_cast<ui64>(thisIP));

        auto line = location->add_line();
        line->set_function_id(reinterpret_cast<ui64>(thisIP));

        auto function = profile.add_function();
        function->set_id(reinterpret_cast<ui64>(thisIP));
    }

    Symbolize(&profile);

    ASSERT_EQ(1, profile.function_size());
    auto function = profile.function(0);

    auto name = profile.string_table(function.name());
    ASSERT_TRUE(name.find("SingleLocation") != TString::npos)
        << "function name is " << name;
}

} // namespace NYT::NProf
