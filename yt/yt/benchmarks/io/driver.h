#pragma once

#include "iotest.h"
#include "statistics.h"
#include "operation.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDriver)

struct IDriver
    : public TRefCounted
{
    virtual void Oneshot(TOperationGenerator generator) = 0;

    virtual void Burst(TOperationGenerator generator) = 0;

    virtual TStatistics GetStatistics() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDriver)

////////////////////////////////////////////////////////////////////////////////

struct TDriverOptionsBase
{
    TInstant Start;
    std::vector<TFile> Files;
    size_t MaxBlockSize = 4096;
    bool Validate = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TMemcpyDriverOptions
    : public TDriverOptionsBase
{ };

IDriverPtr CreateMemcpyDriver(TMemcpyDriverOptions options);

////////////////////////////////////////////////////////////////////////////////

#if 0
struct TRwDriverOptions
    : public TDriverOptionsBase
{ };

IDriverPtr CreateRwDriver(TRwDriverOptions options);
#endif

////////////////////////////////////////////////////////////////////////////////

struct TPrwDriverOptions
    : public TDriverOptionsBase
{ };

IDriverPtr CreatePrwDriver(TPrwDriverOptions options);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TPrwv2DriverConfig)

struct TPrwv2DriverConfig
    : public NYTree::TYsonSerializable
{
    bool HighPriority;
    bool Sync;
    bool DataSync;

    TPrwv2DriverConfig()
    {
        RegisterParameter("high_priority", HighPriority)
            .Default(false);
        RegisterParameter("sync", Sync)
            .Default(false);
        RegisterParameter("data_sync", DataSync)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TPrwv2DriverConfig)

struct TPrwv2DriverOptions
    : public TDriverOptionsBase
{
    TPrwv2DriverConfigPtr Config;
};

IDriverPtr CreatePrwv2Driver(TPrwv2DriverOptions options);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUserspaceGetevents,
    (Never)
    (Optional)
    (Always)
);

DECLARE_REFCOUNTED_STRUCT(TAsyncDriverConfig)

struct TAsyncDriverConfig
    : public NYTree::TYsonSerializable
{
    int QueueSize = 32;
    int BatchSize = 1;
    EUserspaceGetevents UserspaceGetevents;

    TAsyncDriverConfig()
    {
        RegisterParameter("queue_size", QueueSize)
            .Default(64);
        RegisterParameter("batch_size", BatchSize)
            .Default(1);
        RegisterParameter("userspace_getevents", UserspaceGetevents)
            .Default(EUserspaceGetevents::Never);
    }
};

DEFINE_REFCOUNTED_TYPE(TAsyncDriverConfig)

struct TAsyncDriverOptions
    : public TDriverOptionsBase
{
    TAsyncDriverConfigPtr Config;
};

IDriverPtr CreateAsyncDriver(TAsyncDriverOptions options);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TUringDriverConfig)

struct TUringDriverConfig
    : public NYTree::TYsonSerializable
{
    int QueueSize = 64;
    int BatchSize = 6;
    bool FixedFiles = false;
    bool FixedBuffers = false;
    bool Polling = false;
    bool UseKernelSQThread = false;
    bool KernelSQThreadAffinity = -1;

    TUringDriverConfig()
    {
        RegisterParameter("queue_size", QueueSize)
            .Default(64);
        RegisterParameter("batch_size", BatchSize)
            .Default(1);
        RegisterParameter("fixed_files", FixedFiles)
            .Default(false);
        RegisterParameter("fixed_buffers", FixedBuffers)
            .Default(false);
        RegisterParameter("polling", Polling)
            .Default(false);
        RegisterParameter("kernel_sq_thread", UseKernelSQThread)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TUringDriverConfig)

struct TUringDriverOptions
    : public TDriverOptionsBase
{
    TUringDriverConfigPtr Config;
};

IDriverPtr CreateUringDriver(TUringDriverOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
