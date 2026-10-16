#pragma once

#include <contrib/libs/h3/h3lib/include/h3api.h>

// NOTE: as of H3 4.x, all api methods return H3Error,
// which will be ignored by this compatibility layer

#define PROVIDE_H3_COMPAT(method, result, arg)  \
    inline result method(arg in) {              \
        result out;                             \
        ::method(in, &out);                     \
        return out;                             \
    }

#define PROVIDE_H3_COMPAT2(method,result, arg1, arg2)  \
    inline result method(arg1 in1, arg2 in2) {          \
        result out;                                     \
        ::method(in1, in2, &out);                       \
        return out;                                     \
    }

PROVIDE_H3_COMPAT(cellAreaM2, double, H3Index);
PROVIDE_H3_COMPAT(cellAreaRads2, double, H3Index);
PROVIDE_H3_COMPAT(getDirectedEdgeDestination, H3Index, H3Index)
PROVIDE_H3_COMPAT(getDirectedEdgeOrigin, H3Index, H3Index)
PROVIDE_H3_COMPAT(getHexagonAreaAvgKm2, double, int)
PROVIDE_H3_COMPAT(getHexagonAreaAvgM2, double, int)
PROVIDE_H3_COMPAT(getHexagonEdgeLengthAvgKm, double, int);
PROVIDE_H3_COMPAT(getHexagonEdgeLengthAvgM, double, int);
PROVIDE_H3_COMPAT(getNumCells, int64_t, int);
PROVIDE_H3_COMPAT(maxFaceCount, int, H3Index);
PROVIDE_H3_COMPAT(maxGridDiskSize, int64_t, int)
PROVIDE_H3_COMPAT(stringToH3, H3Index, const char*)

PROVIDE_H3_COMPAT2(areNeighborCells, int, H3Index, H3Index)
PROVIDE_H3_COMPAT2(cellToCenterChild, H3Index, H3Index, int)
PROVIDE_H3_COMPAT2(cellToChildrenSize, int64_t, H3Index, int)
PROVIDE_H3_COMPAT2(cellToParent, H3Index, H3Index, int)
PROVIDE_H3_COMPAT2(cellsToDirectedEdge, H3Index, H3Index, H3Index)
PROVIDE_H3_COMPAT2(gridPathCellsSize, int64_t, H3Index, H3Index)

inline double exactEdgeLengthRads(H3Index edge) {
    double out;
    ::edgeLengthRads(edge, &out);
    return out;
}

inline double exactEdgeLengthKm(H3Index edge) {
    double out;
    ::edgeLengthKm(edge, &out);
    return out;
}

inline double exactEdgeLengthM(H3Index edge) {
    double out;
    ::edgeLengthM(edge, &out);
    return out;
}

inline double distanceKm(const LatLng* a, const LatLng* b) {
    return greatCircleDistanceKm(a, b);
}

inline double distanceM(const LatLng* a, const LatLng* b) {
    return greatCircleDistanceM(a, b);
}

inline double distanceRads(const LatLng* a, const LatLng* b) {
    return greatCircleDistanceRads(a, b);
}
