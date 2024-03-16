
#include <library/cpp/yt/logging/logger.h>
#include <yt/yt/core/misc/error.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/systest/test_home.h>

namespace NYT::NTest {

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
    int32_t id;
    {
        auto guard = Guard(Lock_);
        id = UniformIntDistribution_(Engine_);
    }
    const int Length = 256;
    char timebuf[Length];
    std::fill(timebuf, timebuf + Length, 0);

    FillTimeBuf("%Y%m%d-%H%M", Length, timebuf);

    char buf[Length];
    std::snprintf(buf, Length, "%s-%04x", timebuf, id);

    return buf;
}

TString TTestHome::GenerateShortRandomId()
{
    int16_t id;
    {
        auto guard = Guard(Lock_);
        id = UniformShortDistribution_(Engine_);
    }

    const int Length = 256;
    char timebuf[Length];
    std::fill(timebuf, timebuf + Length, 0);

    FillTimeBuf("%H%M", Length, timebuf);

    char buf[Length];
    std::snprintf(buf, Length, "%s-%02hx", timebuf, id);

    return buf;
}

///////////////////////////////////////////////////////////////////////////////

TTestHome::TTestHome(IClientPtr client, const THomeConfig& config)
    : Config_(config)
    , Client_(client)
    , Engine_(RandDevice_())
    , UniformShardDistribution_(0, Config_.IntervalShards - 1)
{
}

void TTestHome::Init()
{
    Dir_ = Config_.HomeDirectory + "/" + GenerateFullRandomId();

    CoreTable_ = Dir_ + "/core";

    TCreateOptions options;
    if (Config_.Ttl != TDuration::Zero()) {
        TInstant expirationTime = TInstant::Now() + Config_.Ttl;
        NYT::TNode::TMapType attributes;
        attributes["expiration_time"] = TNode(expirationTime.MilliSeconds());
        options = options.Attributes(TNode(attributes));
    }

    Client_->Create(Dir_, ENodeType::NT_MAP, options);
    Client_->Create(ValidatorsDir(), ENodeType::NT_MAP);
    Client_->Create(CoreTable_, ENodeType::NT_TABLE);
    for (int i = 0; i < Config_.IntervalShards; ++i) {
        Client_->Create(IntervalsDir(i), ENodeType::NT_MAP);
    }

    NLogging::TLogger Logger("test");
    YT_LOG_INFO("Initialized test home (Directory: %v)", Dir_);
}

TString TTestHome::StderrTable(const TString& operationName)
{
    return Dir_ + "/" + operationName + "_stderr";
}

TString TTestHome::TablePath(const TString& tableName, int retryAttempt) const
{
    return Dir_ + "/" + tableName + "_" + std::to_string(retryAttempt);
}

TString TTestHome::IntervalsDir(int shard) const
{
    return Dir_ + "/intervals_" + std::to_string(shard);
}

TString TTestHome::CreateIntervalPath(const TString& name, int index, int retryAttempt)
{
    int shardId = 0;
    {
        auto guard = Guard(Lock_);
        shardId = UniformShardDistribution_(Engine_);
    }
    return IntervalsDir(shardId)  +
        "/" +
        name +
        "_" +
        std::to_string(index) +
        "_" +
        std::to_string(retryAttempt) +
        "_" +
        GenerateShortRandomId();
}

}  // namespace NYT::NTest
