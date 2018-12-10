#include <yt/core/test_framework/framework.h>

#include <yt/server/skynet_manager/rb_torrent.h>

#include <yt/core/crypto/crypto.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NSkynetManager {
namespace {

using namespace NYT::NYTree;
using namespace NYT::NCrypto;

////////////////////////////////////////////////////////////////////////////////

TEST(TRbTorrentTest, Sample)
{
    TString file1Content = "some content";
    TString file2Content = TString("some big and nice content") * 1024 * 1024;

    ASSERT_EQ(25 * 1024 * 1024, file2Content.Size());

    NSkynetManager::NProto::TResource resource;

    auto& file1 = *resource.add_files();
    file1.set_filename("file1");
    file1.set_file_size(file1Content.Size());

    auto md5 = TMD5Hasher().Append(file1Content).GetDigest();
    file1.set_md5sum(TString(md5.data(), md5.size()));
    auto sha1 = TSha1Hasher().Append(file1Content).GetDigest();
    file1.set_sha1sum(TString(sha1.data(), sha1.size()));

    auto& file2 = *resource.add_files();
    file2.set_filename("path/to/deep/file2");
    file2.set_file_size(file2Content.Size());
    md5 = TMD5Hasher().Append(file2Content).GetDigest();
    file2.set_md5sum(TString(md5.data(), md5.size()));
    
    for (int i = 0; i < file2Content.Size(); i += 4 * 1024 * 1024) {
        auto sha1 = TSha1Hasher()
            .Append(TStringBuf(file2Content).SubStr(i, 4 * 1024 * 1024))
            .GetDigest();
        *file2.mutable_sha1sum() += TString(sha1.data(), sha1.size());
    }

    auto resourceDescription = ConvertResource(resource, true, true);
    ASSERT_EQ("6acc4f8402a635ff21b5cc4043d95768c7c5af47", resourceDescription.ResourceId);
}

TEST(TRbTorrentTest, EmptyFiles)
{
    NSkynetManager::NProto::TResource resource;

    auto zeroMd5 = TMD5Hasher().GetDigest();

    auto& file1 = *resource.add_files();
    file1.set_file_size(0);
    file1.set_filename("a/b/c/file1");
    file1.set_md5sum(TString(zeroMd5.data(), zeroMd5.size()));

    auto& file2 = *resource.add_files();
    file2.set_file_size(0);
    file2.set_filename("a/b/d/file2");
    file2.set_md5sum(TString(zeroMd5.data(), zeroMd5.size()));

    auto resourceDescription = ConvertResource(resource, true, true);
    ASSERT_EQ("2c337a0f3fcb40715cf8e8eba3a18ea1c071972f", resourceDescription.ResourceId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSkynetManager
