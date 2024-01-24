
#include <library/cpp/yt/logging/logger.h>
#include <yt/yt/core/misc/error.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/systest/test_home.h>

namespace NYT::NTest {

TTestHome::TTestHome(IClientPtr client, const TString& homeDirectory, TDuration ttl)
    : HomeDirectory_(homeDirectory)
    , Ttl_(ttl)
    , Client_(client)
    , Engine_(RandDevice_())
{
}

static void FillTimeBuf(const char* fmt, int len, char* timebuf)
{
    time_t currentTime = time(nullptr);
    struct tm* curGmTime = gmtime(&currentTime);

    if (curGmTime == nullptr) {
        THROW_ERROR_EXCEPTION("gmtime() failed, error: %s", strerror(errno));
    }

    strftime(timebuf, len, fmt, curGmTime);
}

TString TTestHome::GenerateFullRandomId()
{
    const int Length = 256;
    char timebuf[Length];
    std::fill(timebuf, timebuf + Length, 0);

    FillTimeBuf("%Y%m%d-%H%M", Length, timebuf);

    char buf[Length];
    std::snprintf(buf, Length, "%s-%04x", timebuf, UniformIntDistribution_(Engine_));

    return buf;
}

TString TTestHome::GenerateShortRandomId()
{
    const int Length = 256;
    char timebuf[Length];
    std::fill(timebuf, timebuf + Length, 0);

    FillTimeBuf("%H%M", Length, timebuf);

    char buf[Length];
    std::snprintf(buf, Length, "%s-%02hx", timebuf, UniformShortDistribution_(Engine_));

    return buf;
}

///////////////////////////////////////////////////////////////////////////////

TTestHome::TTestHome(IClientPtr client, const TString& homeDirectory)
    : HomeDirectory_(homeDirectory)
    , Client_(client)
    , Engine_(RandDevice_())
{
}

void TTestHome::Init()
{
    Dir_ = HomeDirectory_ + "/" + GenerateFullRandomId();

    CoreTable_ = Dir_ + "/core";
    StderrTable_ = Dir_ + "/stderr";

    TCreateOptions options;
    if (Ttl_ != TDuration::Zero()) {
        TInstant expirationTime = TInstant::Now() + Ttl_;
        NYT::TNode::TMapType attributes;
        attributes["expiration_time"] = TNode(expirationTime.MilliSeconds());
        options = options.Attributes(TNode(attributes));
    }

    Client_->Create(Dir_, ENodeType::NT_MAP, options);
    Client_->Create(ValidatorsDir(), ENodeType::NT_MAP);
    Client_->Create(CoreTable_, ENodeType::NT_TABLE);
    Client_->Create(StderrTable_, ENodeType::NT_TABLE);

    NLogging::TLogger Logger("test");
    YT_LOG_INFO("Initialized test home (Directory: %v)", Dir_);
}

TString TTestHome::TablePath(const TString& tableName)
{
    return Dir_ + "/" + tableName;
}

TString TTestHome::CreateIntervalPath(const TString& name, int index)
{
    return Dir_ + "/" + name + "_" + GenerateShortRandomId() + "_" + std::to_string(index);
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
