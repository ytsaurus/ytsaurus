#include <yt/core/test_framework/framework.h>

#include <yt/server/skynet_manager/rb_torrent.h>

#include <yt/core/crypto/crypto.h>

#include <yt/core/ytree/fluent.h>

using namespace NYT;
using namespace NYT::NYTree;
using namespace NYT::NSkynetManager;

TEST(TRbTorrentTest, Sample)
{
    TString file1 = "some content";
    TString file2 = TString("some big and nice content") * 1024 * 1024;

    ASSERT_EQ(25 * 1024 * 1024, file2.Size());

    TFileMeta file1Meta;
    file1Meta.FileSize = file1.Size();
    file1Meta.MD5 = TMD5Hasher().Append(file1).GetDigest();
    file1Meta.SHA1.emplace_back(TSHA1Hasher().Append(file1).GetDigest());

    TFileMeta file2Meta;
    file2Meta.FileSize = file2.Size();
    file2Meta.MD5 = TMD5Hasher().Append(file2).GetDigest();
    for (int i = 0; i < file2.Size(); i += 4 * 1024 * 1024) {
        file2Meta.SHA1.emplace_back(
                TSHA1Hasher()
                        .Append(TStringBuf(file2).SubStr(i, 4 * 1024 * 1024))
                        .GetDigest());
    }

    TSkynetShareMeta meta;
    meta.Files["file1"] = file1Meta;
    meta.Files["path/to/deep/file2"] = file2Meta;

    auto resource = GenerateResource(meta);
    ASSERT_EQ("rbtorrent:6acc4f8402a635ff21b5cc4043d95768c7c5af47", resource.RbTorrentId);
}

TEST(TRbTorrentTest, EmptyFiles)
{
    TFileMeta file1Meta;
    file1Meta.FileSize = 0;
    file1Meta.MD5 = TMD5Hasher().GetDigest();

    TFileMeta file2Meta;
    file2Meta.FileSize = 0;
    file2Meta.MD5 = TMD5Hasher().GetDigest();

    TSkynetShareMeta meta;
    meta.Files["a/b/c/file1"] = file1Meta;
    meta.Files["a/b/d/file2"] = file2Meta;

    auto resource = GenerateResource(meta);
    ASSERT_EQ("rbtorrent:2c337a0f3fcb40715cf8e8eba3a18ea1c071972f", resource.RbTorrentId);
}
