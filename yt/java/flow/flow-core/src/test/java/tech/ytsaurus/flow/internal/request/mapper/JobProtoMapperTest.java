package tech.ytsaurus.flow.internal.request.mapper;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.internal.resource.CompanionResourceInstanceReference;
import tech.ytsaurus.flow.rpc.TCompanionResourceInstanceReference;
import tech.ytsaurus.flow.rpc.TJobInfo;
import tech.ytsaurus.flow.rpc.TReqPutJob;
import tech.ytsaurus.flow.stream.FlowStreamsContext;
import tech.ytsaurus.flow.utils.ProtoUtils;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobProtoMapperTest {
    private static final GUID JOB_ID = GUID.valueOf("1-2-3-4");
    private static final GUID INCARNATION = GUID.valueOf("5-6-7-8");
    private static final GUID DEPENDENCY_INCARNATION = GUID.valueOf("9-a-b-c");
    private static final String COMPUTATION_ID = "computation";

    private final JobProtoMapper mapper = new JobProtoMapper(new FlowStreamsContext(Map.of()));

    @Test
    void putJobPreservesExactResourceReferences() {
        var direct = TCompanionResourceInstanceReference.newBuilder()
                .setResourceId("dictionary")
                .setIncarnationId(ProtoUtils.toProto(INCARNATION))
                .setConfigurationGeneration(11)
                .setAlias("view")
                .build();
        var transitive = TCompanionResourceInstanceReference.newBuilder()
                .setResourceId("pool")
                .setIncarnationId(ProtoUtils.toProto(DEPENDENCY_INCARNATION))
                .setConfigurationGeneration(29)
                .build();

        var job = mapper.fromProto(request(jobInfo(List.of(direct, transitive))));

        assertEquals(JOB_ID, job.getJobId());
        assertEquals(COMPUTATION_ID, job.getComputationId());
        assertEquals(List.of(
                new CompanionResourceInstanceReference("dictionary", INCARNATION, 11, "view"),
                new CompanionResourceInstanceReference("pool", DEPENDENCY_INCARNATION, 29, null)
        ), job.getCompanionResources());
    }

    @Test
    void jobInfoKeepsTransitiveReferenceUnaliased() {
        var reference = TCompanionResourceInstanceReference.newBuilder()
                .setResourceId("pool")
                .setIncarnationId(ProtoUtils.toProto(INCARNATION))
                .setConfigurationGeneration(7)
                .build();

        var job = mapper.fromProto(JOB_ID, COMPUTATION_ID, jobInfo(List.of(reference)));

        assertEquals(List.of(new CompanionResourceInstanceReference("pool", INCARNATION, 7, null)),
                job.getCompanionResources());
        assertNull(job.getCompanionResources().get(0).alias());
    }

    @Test
    void jobsWithoutResourcesHaveEmptyReferences() {
        var info = jobInfo(List.of());

        assertTrue(mapper.fromProto(request(info)).getCompanionResources().isEmpty());
        assertTrue(mapper.fromProto(JOB_ID, COMPUTATION_ID, info).getCompanionResources().isEmpty());
    }

    private static TJobInfo jobInfo(List<TCompanionResourceInstanceReference> references) {
        var emptyMap = YsonUtils.protoFromYTree(YTree.mapBuilder().buildMap());
        return TJobInfo.newBuilder()
                .setSpec(emptyMap)
                .setDynamicSpec(emptyMap)
                .addAllCompanionResources(references)
                .build();
    }

    private static TReqPutJob request(TJobInfo jobInfo) {
        return TReqPutJob.newBuilder()
                .setRequestId(ProtoUtils.toProto(GUID.valueOf("d-e-f-10")))
                .setJobId(ProtoUtils.toProto(JOB_ID))
                .setComputationId(COMPUTATION_ID)
                .setJobInfo(jobInfo)
                .build();
    }
}
