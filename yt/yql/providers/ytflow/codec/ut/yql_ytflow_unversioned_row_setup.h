#pragma once

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/fluent.h>

#include <functional>


namespace NYql::NYtflow::NCodec::NTest {

struct TUnversionedRowSetup {
public:
    NYT::NTableClient::TTableSchemaPtr YtSchema;
    NYT::NTableClient::TRowBufferPtr RowBuffer;

public:
    TUnversionedRowSetup();

    // static interface
    NYT::NTableClient::TTableSchemaPtr BuildYtSchema() = delete;
    NYT::NTableClient::TUnversionedRow BuildUnversionedRow() = delete;

    void AssertExpectedUnversionedRow(
        NYT::NTableClient::TUnversionedRow unversionedRow) const = delete;

protected:
    using TProduceCallback = std::function<void(NYT::NYTree::TFluentAny)>;
    using TConsumeCallback = std::function<void(NYT::NYson::TYsonPullParser&)>;

protected:
    template <typename TValue>
    void SetSimpleValue(
        NYT::NTableClient::TMutableUnversionedRow& mutableUnversionedRow,
        TStringBuf name,
        TValue&& value);

    void SetCompositeValue(
        NYT::NTableClient::TMutableUnversionedRow& mutableUnversionedRow,
        TStringBuf name,
        TProduceCallback&& callback
    );

    void SetUnversionedValue(
        NYT::NTableClient::TMutableUnversionedRow& mutableUnversionedRow,
        TStringBuf name,
        NYT::NTableClient::TUnversionedValue&& value);

    NYT::NTableClient::TUnversionedValue GetUnversionedValue(
        const NYT::NTableClient::TUnversionedRow& unversionedRow,
        TStringBuf name) const;

    template <typename TValue>
    void AssertSimpleValue(
        const NYT::NTableClient::TUnversionedRow& unversionedRow,
        TStringBuf name,
        TValue&& value) const;

    void AssertCompositeValue(
        const NYT::NTableClient::TUnversionedRow& unversionedRow,
        TStringBuf name,
        TConsumeCallback&& consumeCallback) const;
};

struct TUnversionedRowSetupFull: public TUnversionedRowSetup {
public:
    TUnversionedRowSetupFull();

    NYT::NTableClient::TTableSchemaPtr BuildYtSchema();
    NYT::NTableClient::TUnversionedRow BuildUnversionedRow();

    void AssertExpectedUnversionedRow(
        NYT::NTableClient::TUnversionedRow unversionedRow) const;
};

struct TUnversionedRowSetupLarge: public TUnversionedRowSetup {
public:
    TUnversionedRowSetupLarge();

    NYT::NTableClient::TTableSchemaPtr BuildYtSchema();
    NYT::NTableClient::TUnversionedRow BuildUnversionedRow();

    void AssertExpectedUnversionedRow(
        NYT::NTableClient::TUnversionedRow unversionedRow) const;
};

struct TUnversionedRowSetupLargeOptional: public TUnversionedRowSetup {
public:
    TUnversionedRowSetupLargeOptional();

    NYT::NTableClient::TTableSchemaPtr BuildYtSchema();
    NYT::NTableClient::TUnversionedRow BuildUnversionedRow();

    void AssertExpectedUnversionedRow(
        NYT::NTableClient::TUnversionedRow unversionedRow) const;
};

struct TUnversionedRowSetupSmall: public TUnversionedRowSetup {
public:
    TUnversionedRowSetupSmall();

    NYT::NTableClient::TTableSchemaPtr BuildYtSchema();
    NYT::NTableClient::TUnversionedRow BuildUnversionedRow();

    void AssertExpectedUnversionedRow(
        NYT::NTableClient::TUnversionedRow unversionedRow) const;
};

} // namespace NYql::NYtflow::NCodec::NTest
