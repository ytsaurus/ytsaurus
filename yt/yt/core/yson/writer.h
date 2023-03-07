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
        bool booleanAsString = false,
        int indent = DefaultIndent);

    // IYsonConsumer overrides.
    virtual void OnStringScalar(TStringBuf value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;

    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;

    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(TStringBuf key) override;
    virtual void OnEndMap() override;

    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

    using IYsonConsumer::OnRaw;
    virtual void OnRaw(TStringBuf yson, EYsonType type = EYsonType::Node) override;

    virtual void Flush() override;

    int GetDepth() const;

protected:
    IOutputStream* const Stream_;
    const EYsonFormat Format_;
    const EYsonType Type_;
    const bool EnableRaw_;
    const bool BooleanAsString_;
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
        bool enableRaw = true,
        bool booleanAsString = false);

    // IYsonConsumer overrides.
    virtual void OnStringScalar(TStringBuf value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;

    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;

    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(TStringBuf key) override;
    virtual void OnEndMap() override;

    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

    using IYsonConsumer::OnRaw;
    virtual void OnRaw(TStringBuf yson, EYsonType type = EYsonType::Node) override;

    virtual void Flush() override;

    int GetDepth() const;

protected:
    IOutputStream* const Stream_;
    const EYsonType Type_;
    const bool EnableRaw_;
    const bool BooleanAsString_;

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
    bool booleanAsString,
    int indent = TYsonWriter::DefaultIndent);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

