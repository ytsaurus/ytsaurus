
#include <yt/yt/core/misc/error.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/systest/test_home.h>

namespace NYT::NTest {

TTestHome::TTestHome(IClientPtr client, const TString& homeDirectory)
    : HomeDirectory_(homeDirectory)
    , Client_(client)
    , Engine_(RandDevice_())
{
}

void TTestHome::Init()
{
    const int BUF_LEN = 256;
    char timebuf[BUF_LEN];
    std::fill(timebuf, timebuf + BUF_LEN, 0);

    time_t currentTime = time(nullptr);
    struct tm* curGmTime = gmtime(&currentTime);

    if (curGmTime == nullptr) {
        THROW_ERROR_EXCEPTION("gmtime() failed, error: %s", strerror(errno));
    }

    strftime(timebuf, BUF_LEN, "%Y%m%d-%H%M", curGmTime);

    char buf[BUF_LEN];
    std::snprintf(buf, BUF_LEN, "/%s-%04x", timebuf, UniformIntDistribution_(Engine_));

    Dir_ = HomeDirectory_ + buf;

    CoreTable_ = Dir_ + "/core";
    StderrTable_ = Dir_ + "/stderr";

    Client_->Create(Dir_, ENodeType::NT_MAP);
    Client_->Create(CoreTable_, ENodeType::NT_TABLE);
    Client_->Create(StderrTable_, ENodeType::NT_TABLE);
}

TString TTestHome::CreateRandomTablePath()
{
    const int BUF_LEN = 64;
    char buf[BUF_LEN];
    std::fill(buf, buf + BUF_LEN, 0);
    std::snprintf(buf, BUF_LEN, "/%04x", UniformIntDistribution_(Engine_));

    return Dir_ + buf;
}

}  // namespace NYT::NTest
