#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/sparse_coredump/sparse_coredump.h>

#include <util/system/tempfile.h>
#include <util/stream/file.h>

namespace NYT::NCoreDump {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCoreDumpTest, WriteSparseFile)
{
    auto filename = "./coredump";
    TTempFile tmp(filename);

    TStringStream ss;
    for (size_t i = 0; i < 16_KB; ++i) {
        ss.Write('f');
    }
    for (size_t i = 0; i < 16_KB; ++i) {
        ss.Write(0);
    }
    for (size_t i = 0; i < 16_KB; ++i) {
        ss.Write('f');
    }
    for (size_t i = 0; i < 16_KB; ++i) {
        ss.Write(0);
    }

    TFile coreFile(filename, CreateNew | WrOnly | Seq | CloseOnExec);
    auto dump = ss.Str();
    TStringInput si(dump);

    EXPECT_EQ(64_KB, static_cast<size_t>(WriteSparseCoreDump(&si, &coreFile)));

    TFileInput input(filename);
    EXPECT_TRUE(input.ReadAll() == dump);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCoreDump
