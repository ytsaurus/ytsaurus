#pragma once

#include "builder.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>

/**
 * Usage:
 * I. For pull:
 *     1. Just fill json by metrics using TSolomonJsonBuilder on demand
 *
 * II. For push (manual):
 *     1. Full your sensors via TPushRequestBuilder methods AddSensor()/AddDerivSensor()
 *     2. Call TPushRequestBuilder::PushToSolomon()
 */
