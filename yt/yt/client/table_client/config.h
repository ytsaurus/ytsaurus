#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/misc/singleton.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TRetentionConfig
    : public virtual NYTree::TYsonStruct
{
public:
    int MinDataVersions;
    int MaxDataVersions;
    TDuration MinDataTtl;
    TDuration MaxDataTtl;
    bool IgnoreMajorTimestamp;

    REGISTER_YSON_STRUCT(TRetentionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRetentionConfig)

TString ToString(const TRetentionConfigPtr& obj);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESamplingMode,
    ((Row)               (1))
    ((Block)             (2))
);

class TChunkReaderConfig
    : public virtual NChunkClient::TBlockFetcherConfig
{
public:
    std::optional<ESamplingMode> SamplingMode;
    std::optional<double> SamplingRate;
    std::optional<ui64> SamplingSeed;

    static TChunkReaderConfigPtr GetDefault()
    {
        return LeakyRefCountedSingleton<TChunkReaderConfig>();
    }

    REGISTER_YSON_STRUCT(TChunkReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterTestingOptions
    : public NYTree::TYsonStruct
{
public:
    //! If true, unsupported chunk feature is added to chunk meta.
    bool AddUnsupportedFeature;

    REGISTER_YSON_STRUCT(TChunkWriterTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterConfig
    : public NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;
    i64 MaxSegmentValueCount;

    i64 MaxBufferSize;

    i64 MaxRowWeight;

    i64 MaxKeyWeight;

    //! This limits ensures that chunk index is dense enough
    //! e.g. to produce good slices for reduce.
    i64 MaxDataWeightBetweenBlocks;

    i64 MaxKeyFilterSize;

    double SampleRate;

    double KeyFilterFalsePositiveRate;

    TChunkWriterTestingOptionsPtr TestingOptions;

    REGISTER_YSON_STRUCT(TChunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableReaderConfig
    : public virtual NChunkClient::TMultiChunkReaderConfig
    , public virtual TChunkReaderConfig
{
public:
    bool SuppressAccessTracking;
    bool SuppressExpirationTimeoutRenewal;
    EUnavailableChunkStrategy UnavailableChunkStrategy;
    NChunkClient::EChunkAvailabilityPolicy ChunkAvailabilityPolicy;
    std::optional<TDuration> MaxReadDuration;

    NTabletClient::TRetryingRemoteDynamicStoreReaderConfigPtr DynamicStoreReader;

    REGISTER_YSON_STRUCT(TTableReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{
    REGISTER_YSON_STRUCT(TTableWriterConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TTypeConversionConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableTypeConversion;
    bool EnableStringToAllConversion;
    bool EnableAllToStringConversion;
    bool EnableIntegralTypeConversion;
    bool EnableIntegralToDoubleConversion;

    TTypeConversionConfig();
};

DEFINE_REFCOUNTED_TYPE(TTypeConversionConfig)

////////////////////////////////////////////////////////////////////////////////

class TInsertRowsFormatConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    bool EnableNullToYsonEntityConversion;

    TInsertRowsFormatConfig();
};

DEFINE_REFCOUNTED_TYPE(TInsertRowsFormatConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderOptions
    : public virtual NYTree::TYsonStruct
{
public:
    bool EnableTableIndex;
    bool EnableRangeIndex;
    bool EnableRowIndex;
    bool DynamicTable;
    bool EnableTabletIndex;
    bool EnableKeyWidening;

    static TChunkReaderOptionsPtr GetDefault()
    {
        return LeakyRefCountedSingleton<TChunkReaderOptions>();
    }

    REGISTER_YSON_STRUCT(TChunkReaderOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderOptions)

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterOptions
    : public virtual NChunkClient::TEncodingWriterOptions
{
public:
    bool ValidateSorted;
    bool ValidateRowWeight;
    bool ValidateKeyWeight;
    bool ValidateDuplicateIds;
    bool ValidateUniqueKeys;
    bool ExplodeOnValidationError;
    bool ValidateColumnCount;
    bool EvaluateComputedColumns;
    bool EnableSkynetSharing;
    bool ReturnBoundaryKeys;
    bool CastAnyToComposite = false;
    NYTree::INodePtr CastAnyToCompositeNode;

    ETableSchemaModification SchemaModification;

    EOptimizeFor OptimizeFor;

    //! Maximum number of heavy columns in approximate statistics.
    int MaxHeavyColumns;

    void EnableValidationOptions();

    REGISTER_YSON_STRUCT(TChunkWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

struct TRowBatchReadOptions
{
    //! The desired number of rows to read.
    //! This is just an estimate; not all readers support this limit.
    i64 MaxRowsPerRead = 10000;

    //! The desired data weight to read.
    //! This is just an estimate; not all readers support this limit.
    i64 MaxDataWeightPerRead = 16_MB;

    //! If true then the reader may return a columnar batch.
    //! If false then the reader must return a non-columnar batch.
    bool Columnar = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
