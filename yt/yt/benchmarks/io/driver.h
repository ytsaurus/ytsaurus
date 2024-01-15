#pragma once

#include "iotest.h"
#include "statistics.h"
#include "operation.h"

#include <yt/yt/core/ytree/yson_struct.h>

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
    : public NYTree::TYsonStruct
{
    bool HighPriority;
    bool Sync;
    bool DataSync;

    REGISTER_YSON_STRUCT(TPrwv2DriverConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("high_priority", &TThis::HighPriority)
            .Default(false);
        registrar.Parameter("sync", &TThis::Sync)
            .Default(false);
        registrar.Parameter("data_sync", &TThis::DataSync)
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
    : public NYTree::TYsonStruct
{
    int QueueSize = 32;
    int BatchSize = 1;
    EUserspaceGetevents UserspaceGetevents;

    REGISTER_YSON_STRUCT(TAsyncDriverConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("queue_size", &TThis::QueueSize)
            .Default(64);
        registrar.Parameter("batch_size", &TThis::BatchSize)
            .Default(1);
        registrar.Parameter("userspace_getevents", &TThis::UserspaceGetevents)
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
    : public NYTree::TYsonStruct
{
    int QueueSize = 64;
    int BatchSize = 6;
    bool FixedFiles = false;
    bool FixedBuffers = false;
    bool Polling = false;
    bool UseKernelSQThread = false;
    bool KernelSQThreadAffinity = -1;

    REGISTER_YSON_STRUCT(TUringDriverConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("queue_size", &TThis::QueueSize)
            .Default(64);
        registrar.Parameter("batch_size", &TThis::BatchSize)
            .Default(1);
        registrar.Parameter("fixed_files", &TThis::FixedFiles)
            .Default(false);
        registrar.Parameter("fixed_buffers", &TThis::FixedBuffers)
            .Default(false);
        registrar.Parameter("polling", &TThis::Polling)
            .Default(false);
        registrar.Parameter("kernel_sq_thread", &TThis::UseKernelSQThread)
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
