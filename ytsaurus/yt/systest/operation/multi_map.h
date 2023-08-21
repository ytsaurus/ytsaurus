#pragma once

#include <yt/systest/operation.h>

namespace NYT::NTest {

class TFilterMultiMapper : public IMultiMapper
{
public:
    TFilterMultiMapper(const TTable& input, int columnIndex, TNode value);
    TFilterMultiMapper(const TTable& input, const NProto::TFilterMultiMapper& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;
    virtual void ToProto(NProto::TMultiMapper* proto) const override;

    virtual std::vector<std::vector<TNode>> Run(TCallState* state, TRange<TNode> input) const override;

private:
    const int ColumnIndex_;
    TNode Value_;
    std::vector<int> InputColumnIndex_;

    void PopulateInputColumns();
};

/////////////////////////////////////////////////////////////////////////////////////////

class TSingleMultiMapper : public IMultiMapper
{
public:
    TSingleMultiMapper(const TTable& input, std::unique_ptr<IRowMapper> inner);
    TSingleMultiMapper(const TTable& input, const NProto::TSingleMultiMapper& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;
    virtual void ToProto(NProto::TMultiMapper* proto) const override;

    virtual std::vector<std::vector<TNode>> Run(TCallState* state, TRange<TNode> input) const override;

private:
    std::unique_ptr<IRowMapper> Inner_;
};

/////////////////////////////////////////////////////////////////////////////////////////

class TRepeatMultiMapper : public IMultiMapper
{
public:
    TRepeatMultiMapper(const TTable& input, int rowCount, std::unique_ptr<IRowMapper> inner);
    TRepeatMultiMapper(const TTable& input, const NProto::TRepeatMultiMapper& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;
    virtual void ToProto(NProto::TMultiMapper* proto) const override;

    virtual std::vector<std::vector<TNode>> Run(TCallState* state, TRange<TNode> input) const override;

private:
    const int Count_;
    std::unique_ptr<const IRowMapper> Inner_;
};

/////////////////////////////////////////////////////////////////////////////////////////

class TCombineMultiMapper : public IMultiMapper
{
public:
    TCombineMultiMapper(
        const TTable& input,
        std::vector<std::unique_ptr<IRowMapper>> singleOperations,
        std::unique_ptr<IMultiMapper> multiOperation);

    TCombineMultiMapper(
        const TTable& input,
        const NProto::TCombineMultiMapper& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;

    virtual std::vector<std::vector<TNode>> Run(TCallState* state, TRange<TNode> input) const override;
    virtual void ToProto(NProto::TMultiMapper* proto) const override;

private:
    std::vector<std::unique_ptr<IRowMapper>> SingleOperations_;
    std::unique_ptr<IMultiMapper> MultiOperation_;

    std::vector<int> InputColumns_;

    void PopulateInputColumns();
};

}  // namespace NYT::NTest
