#pragma once

#include "public.h"
#include "consumer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! A canonical implementation of YSON data stream writer.
class TYsonWriter
    : public TYsonConsumerBase
    , public virtual IFlushableYsonConsumer
    , private TNonCopyable
{
public:
    static constexpr int DefaultIndent = 4;

    //! Initializes an instance.
    /*!
     *  \param stream A stream for writing the YSON data to.
     *  \param format A format used for encoding the data.
     *  \param enableRaw Enables inserting raw portions of YSON as-is, without reparse.
     */
    TYsonWriter(
        IOutputStream* stream,
        EYsonFormat format = EYsonFormat::Binary,
        EYsonType type = EYsonType::Node,
        bool enableRaw = false,
        int indent = DefaultIndent);

    // IYsonConsumer overrides.
    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;

    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;

    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;

    void OnBeginAttributes() override;
    void OnEndAttributes() override;

    using IYsonConsumer::OnRaw;
    void OnRaw(TStringBuf yson, EYsonType type = EYsonType::Node) override;

    void Flush() override;

    int GetDepth() const;

protected:
    IOutputStream* const Stream_;
    const EYsonFormat Format_;
    const EYsonType Type_;
    const bool EnableRaw_;
    const int IndentSize_;

    int Depth_ = 0;
    bool EmptyCollection_ = true;


    void WriteIndent();
    void WriteStringScalar(TStringBuf value);

    void BeginCollection(char ch);
    void CollectionItem();
    void EndCollection(char ch);

    void EndNode();
};

////////////////////////////////////////////////////////////////////////////////

//! An optimized version of YSON stream writer.
/*!
 *  This writer buffers its output so don't forget to call #Flush.
 *  Only binary YSON format is supported.
 */
class TBufferedBinaryYsonWriter
    : public TYsonConsumerBase
    , public virtual IFlushableYsonConsumer
    , private TNonCopyable
{
public:
    //! Initializes an instance.
    /*!
     *  \param stream A stream for writing the YSON data to.
     *  \param format A format used for encoding the data.
     *  \param enableRaw Enables inserting raw portions of YSON as-is, without reparse.
     */
    TBufferedBinaryYsonWriter(
        IOutputStream* stream,
        EYsonType type = EYsonType::Node,
        bool enableRaw = true);

    // IYsonConsumer overrides.
    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;

    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;

    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;

    void OnBeginAttributes() override;
    void OnEndAttributes() override;

    using IYsonConsumer::OnRaw;
    void OnRaw(TStringBuf yson, EYsonType type = EYsonType::Node) override;

    void Flush() override;

    int GetDepth() const;

protected:
    IOutputStream* const Stream_;
    const EYsonType Type_;
    const bool EnableRaw_;

    static constexpr size_t BufferSize = 1024;
    static constexpr size_t MaxSmallStringLength = 256;

    char Buffer_[BufferSize];
    char* const BufferStart_;
    char* const BufferEnd_;
    char* BufferCursor_;

    int Depth_ = 0;

    void EnsureSpace(size_t space);
    void WriteStringScalar(TStringBuf value);
    void BeginCollection(char ch);
    void EndCollection(char ch);
    void EndNode();

};

////////////////////////////////////////////////////////////////////////////////

//! Creates either TYsonWriter or TBufferedBinaryYsonWriter, depending on #format.
std::unique_ptr<IFlushableYsonConsumer> CreateYsonWriter(
    IOutputStream* output,
    EYsonFormat format,
    EYsonType type,
    bool enableRaw,
    int indent = TYsonWriter::DefaultIndent);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

