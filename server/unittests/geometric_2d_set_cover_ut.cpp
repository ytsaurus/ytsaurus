#include <yt/core/test_framework/framework.h>

#include <yp/server/objects/geometric_2d_set_cover.h>

namespace NYP::NServer::NObjects::NGeometric2DSetCover::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
void ValidateCovering(
    const std::vector<TPoint<TRow>>& points,
    const std::vector<TRectangle<TRow>>& rectangles)
{
    THashSet<TPoint<TRow>> coveredPoints;
    for (const auto& rectangle : rectangles) {
        for (auto row : rectangle.Rows) {
            for (auto column : rectangle.Columns) {
                coveredPoints.emplace(row, column);
            }
        }
    }
    for (const auto& point : points) {
        auto it = coveredPoints.find(point);
        EXPECT_TRUE(it != coveredPoints.end());
    }
}

////////////////////////////////////////////////////////////////////////////////

using TTestRow = TString;

TEST(TBuildPerColumnSetCoveringTest, Manual)
{
    std::vector<TPoint<TTestRow>> points{
        {"r5", 3}, {"r5", 4}, {"r5", 1}, {"r5", 2},
        {"r1", 1}, {"r1", 2}, {"r1", 3}, {"r1", 4},
        {"r2", 3000}, {"r2", 100}, {"r2", 0},
        {"r3", 5}, {"r3", 2}, {"r3", 0},
        {"r6", 100}, {"r6", 0}, {"r6", 3000},
        {"r4", 1}, {"r4", 3}, {"r4", 2}, {"r4", 4}};
    {
        auto rectangles = BuildPerColumnSetCovering(points, 10);
        ValidateCovering(points, rectangles);
        auto expectedRectangles = std::vector<TRectangle<TTestRow>>{
            TRectangle<TTestRow>{{"r1", "r4", "r5"}, {1, 2, 3, 4}},
            TRectangle<TTestRow>{{"r2", "r6"}, {0, 100, 3000}},
            TRectangle<TTestRow>{{"r3"}, {0, 2, 5}}};
        EXPECT_THAT(rectangles, testing::UnorderedElementsAreArray(expectedRectangles));
    }
    {
        auto rectangles = BuildPerColumnSetCovering(points, 2);
        ValidateCovering(points, rectangles);
        auto expectedRectangles = std::vector<TRectangle<TTestRow>>{
            TRectangle<TTestRow>{{"r1", "r4"}, {1, 2, 3, 4}},
            TRectangle<TTestRow>{{"r5"}, {1, 2, 3, 4}},
            TRectangle<TTestRow>{{"r2", "r6"}, {0, 100, 3000}},
            TRectangle<TTestRow>{{"r3"}, {0, 2, 5}}};
        EXPECT_THAT(rectangles, testing::UnorderedElementsAreArray(expectedRectangles));
    }
    {
        auto rectangles = BuildPerColumnSetCovering(points, 1);
        ValidateCovering(points, rectangles);
        auto expectedRectangles = std::vector<TRectangle<TTestRow>>{
            TRectangle<TTestRow>{{"r1"}, {1, 2, 3, 4}},
            TRectangle<TTestRow>{{"r4"}, {1, 2, 3, 4}},
            TRectangle<TTestRow>{{"r5"}, {1, 2, 3, 4}},
            TRectangle<TTestRow>{{"r2"}, {0, 100, 3000}},
            TRectangle<TTestRow>{{"r6"}, {0, 100, 3000}},
            TRectangle<TTestRow>{{"r3"}, {0, 2, 5}}};
        EXPECT_THAT(rectangles, testing::UnorderedElementsAreArray(expectedRectangles));
    }
}

TEST(TBuildPerColumnSetCoveringTest, Stress)
{
    auto rowCount = 10;
    auto columnCount = 10;
    for (int pointCount = 1; pointCount < 100; ++pointCount) {
        std::vector<TPoint<TTestRow>> points;
        points.reserve(pointCount);
        for (int pointIndex = 0; pointIndex < pointCount; ++pointIndex) {
            points.emplace_back(
                "row" + std::to_string(RandomNumber<ui32>(rowCount)),
                RandomNumber<ui32>(columnCount));
        }
        for (int maxRowsPerRectangle = 1;
            maxRowsPerRectangle <= pointCount + 1;
            ++maxRowsPerRectangle)
        {
            auto rectangles = BuildPerColumnSetCovering(points, maxRowsPerRectangle);
            ValidateCovering(points, rectangles);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NServer::NObjects::NGeometric2DSetCover::NTests
