#include "stdafx.h"

#include <ytlib/misc/metric.h>

#include <contrib/testing/framework.h>
#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMetricTest : public ::testing::Test
{ };

TEST_F(TMetricTest, ZeroValues)
{
    TMetric metric(50, 100, 10);

    EXPECT_EQ(0, metric.GetMean());
    EXPECT_EQ(0, metric.GetStd());
}


TEST_F(TMetricTest, OneValue)
{
    TMetric metric(50, 100, 10);
    metric.AddValue(75.);

    EXPECT_DOUBLE_EQ(75, metric.GetMean());
    EXPECT_DOUBLE_EQ(0, metric.GetStd());
}

TEST_F(TMetricTest, ManyValues)
{
    TMetric metric(1, 2, 10);

    // from random import randint
    // for s in ["x.push_back(%.3lf);" % uniform(0, 4) for i in xrange(10)]:print s

    yvector<double> x;
    x.push_back(1.907);
    x.push_back(2.259);
    x.push_back(3.374);
    x.push_back(0.313);
    x.push_back(1.125);
    x.push_back(2.751);
    x.push_back(0.715);
    x.push_back(1.467);
    x.push_back(3.252);
    x.push_back(1.986);


    for (int i = 0; i < x.ysize(); ++i) {
        metric.AddValue(x[i]);
    }

    double sum = 0;
    for(int i = 0; i < x.ysize(); ++i) {
        sum += x[i];
    }

    double mean = sum / x.size();

    double sumDeltaSq = 0;
    for (int i = 0; i < x.ysize(); ++i) {
        double delta = (x[i] - mean);
        sumDeltaSq += delta * delta;
    }

    double std = std::sqrt(sumDeltaSq / x.size());

    double eps = 1e-10;
    EXPECT_NEAR(mean, metric.GetMean(), eps);
    EXPECT_NEAR(std, metric.GetStd(), eps);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

