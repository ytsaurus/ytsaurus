#pragma once

#include "private.h"
#include "fair_share_tree_element.h"
#include "fair_share_tree_element_classic.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TVectorFairShareImpl
{
public:
    using TSchedulerElement = NVectorScheduler::TSchedulerElement;
    using TSchedulerElementPtr = NVectorScheduler::TSchedulerElementPtr;
    using TOperationElement = NVectorScheduler::TOperationElement;
    using TOperationElementPtr = NVectorScheduler::TOperationElementPtr;
    using TCompositeSchedulerElement = NVectorScheduler::TCompositeSchedulerElement;
    using TCompositeSchedulerElementPtr = NVectorScheduler::TCompositeSchedulerElementPtr;
    using TPool = NVectorScheduler::TPool;
    using TPoolPtr = NVectorScheduler::TPoolPtr;
    using TRootElement = NVectorScheduler::TRootElement;
    using TRootElementPtr = NVectorScheduler::TRootElementPtr;

    using TDynamicAttributes = NVectorScheduler::TDynamicAttributes;
    using TDynamicAttributesList = NVectorScheduler::TDynamicAttributesList;
    using TUpdateFairShareContext = NVectorScheduler::TUpdateFairShareContext;
    using TFairShareSchedulingStage = NVectorScheduler::TFairShareSchedulingStage;
    using TFairShareContext = NVectorScheduler::TFairShareContext;
    using TSchedulableAttributes = NVectorScheduler::TSchedulableAttributes;
    using TPersistentAttributes = NVectorScheduler::TPersistentAttributes;

    using TRawOperationElementMap = NVectorScheduler::TRawOperationElementMap;
    using TOperationElementMap = NVectorScheduler::TOperationElementMap;

    using TRawPoolMap = NVectorScheduler::TRawPoolMap;
    using TPoolMap = NVectorScheduler::TPoolMap;
};

////////////////////////////////////////////////////////////////////////////////

class TClassicFairShareImpl
{
public:
    using TSchedulerElement = NClassicScheduler::TSchedulerElement;
    using TSchedulerElementPtr = NClassicScheduler::TSchedulerElementPtr;
    using TOperationElement = NClassicScheduler::TOperationElement;
    using TOperationElementPtr = NClassicScheduler::TOperationElementPtr;
    using TCompositeSchedulerElement = NClassicScheduler::TCompositeSchedulerElement;
    using TCompositeSchedulerElementPtr = NClassicScheduler::TCompositeSchedulerElementPtr;
    using TPool = NClassicScheduler::TPool;
    using TPoolPtr = NClassicScheduler::TPoolPtr;
    using TRootElement = NClassicScheduler::TRootElement;
    using TRootElementPtr = NClassicScheduler::TRootElementPtr;

    using TDynamicAttributes = NClassicScheduler::TDynamicAttributes;
    using TDynamicAttributesList = NClassicScheduler::TDynamicAttributesList;
    using TUpdateFairShareContext = NClassicScheduler::TUpdateFairShareContext;
    using TFairShareSchedulingStage = NClassicScheduler::TFairShareSchedulingStage;
    using TFairShareContext = NClassicScheduler::TFairShareContext;
    using TSchedulableAttributes = NClassicScheduler::TSchedulableAttributes;
    using TPersistentAttributes = NClassicScheduler::TPersistentAttributes;

    using TRawOperationElementMap = NClassicScheduler::TRawOperationElementMap;
    using TOperationElementMap = NClassicScheduler::TOperationElementMap;

    using TRawPoolMap = NClassicScheduler::TRawPoolMap;
    using TPoolMap = NClassicScheduler::TPoolMap;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
