#pragma once

#include <yt/systest/operation.h>

namespace NYT::NTest {

// Set a random seed by taking an int value from the input stream.
class TSetSeedRowMapper : public IRowMapper
{
public:
    TSetSeedRowMapper(const TTable& input, int columnIndex, int thisSeed = 0);
    TSetSeedRowMapper(const TTable& input, const NProto::TSetSeedRowMapper& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;
    virtual void ToProto(NProto::TRowMapper* proto) const override;

    virtual std::vector<TNode> Run(TCallState* state, TRange<TNode> input) const override;

private:
    const int InputColumnIndex[1];
    int ThisSeed_;
};

////////////////////////////////////////////////////////////////////////////////

class TGenerateRandomRowMapper : public IRowMapper
{
public:
    TGenerateRandomRowMapper(const TTable& input, TDataColumn output);
    TGenerateRandomRowMapper(const TTable& input, const NProto::TGenerateRandomRowMapper& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;
    virtual void ToProto(NProto::TRowMapper* proto) const override;

    virtual std::vector<TNode> Run(TCallState* state, TRange<TNode> input) const override;

private:
    TDataColumn OutputColumns_[1];

    TNode Generate(TCallState* state) const;
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateColumnsRowMapper : public IRowMapper
{
public:
    TConcatenateColumnsRowMapper(const TTable& input, std::vector<std::unique_ptr<IRowMapper>> operations);
    TConcatenateColumnsRowMapper(const TTable& input, const NProto::TConcatenateColumnsRowMapper& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;
    virtual void ToProto(NProto::TRowMapper* proto) const override;

    virtual std::vector<TNode> Run(TCallState* state, TRange<TNode> input) const override;

private:
    std::vector<std::unique_ptr<IRowMapper>> Operations_;
    std::vector<TDataColumn> OutputColumns_;

    std::vector<int> InputColumns_;
};

}  // namespace NYT::NTest
