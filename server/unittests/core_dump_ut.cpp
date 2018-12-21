#include <yt/core/test_framework/framework.h>

#include <yt/server/core_dump/core_dump.h>

#include <util/system/tempfile.h>
#include <util/stream/file.h>

namespace NYT::NCoreDump {
namespace {

TEST(TCoreDumpTest, WriteSparseFile)
{
    auto filename = "./coredump";
    TTempFile tmp(filename);

    TStringStream ss;
    for (int i = 0; i < 16_KB; ++i) {
        ss.Write('f');
    }
    for (int i = 0; i < 16_KB; ++i) {
        ss.Write(0);
    }
    for (int i = 0; i < 16_KB; ++i) {
        ss.Write('f');
    }
    for (int i = 0; i < 16_KB; ++i) {
        ss.Write(0);
    }

    TFile coreFile(filename, CreateNew | WrOnly | Seq | CloseOnExec);
    auto dump = ss.Str();
    TStringInput si(dump);
    
    EXPECT_EQ(64_KB, WriteSparseCoreDump(&si, &coreFile));

    TFileInput input(filename);
    EXPECT_TRUE(input.ReadAll() == dump);
}

} // namespace
} // namespace NYT::NCoreDump
