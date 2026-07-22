package tech.ytsaurus.flow.internal.request.mapper;

import java.util.ArrayList;
import java.util.HashMap;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.job.Job;
import tech.ytsaurus.flow.request.RequestContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.row.Visit;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.stream.FlowStreamsContext;
import tech.ytsaurus.flow.utils.ProtoUtils;

/**
 * Orchestrator that composes per-type mappers to convert protobuf requests into a domain
 * {@link RequestContext} and related objects.
 */
public class RequestProtoMapper {

    private final FlowStreamsContext streamsContext;
    private final CodecRegistry codecs;

    /**
     * Creates a request mapper with the supplied stream context.
     *
     * @param streamsContext registered streams of the pipeline
     */
    public RequestProtoMapper(FlowStreamsContext streamsContext) {
        this.streamsContext = streamsContext;
        this.codecs = CodecRegistry.getInstance();
    }

    /**
     * Maps a protobuf ProcessBatch request to a {@link RequestContext}.
     *
     * @param request the protobuf request
     * @param job     the job instance
     * @return the request context
     */
    public RequestContext fromProto(TReqProcessBatch request, Job job) {
        var builder = RequestContext.builder();
        // Base fields.
        builder.setComputationId(request.getComputationId());
        GUID jobId = ProtoUtils.fromProto(request.getJobId());
        builder.setJobId(jobId);
        GUID requestId = ProtoUtils.fromProto(request.getRequestId());
        builder.setRequestId(requestId);
        builder.setJob(job);
        var keySchema = job.getGroupBySchema();
        var streamSpecs = job.getStreamSpecs();

        // Streams overrides for source computation.
        if (request.getStreamsCount() > 0) {
            var streamSpecsMapper = new StreamSpecsProtoMapper(streamsContext);
            var streamSpecsOverride = streamSpecsMapper.fromProto(request.getStreamsList());
            streamSpecs = streamSpecsOverride;
            builder.setStreamSpecsOverride(streamSpecsOverride);
        }

        // Messages.
        var extendedMessageMapper = new ExtendedMessageProtoMapper(
                keySchema, streamSpecs, codecs.getKeyCodec()
        );
        var messages = new ArrayList<ExtendedMessage>(request.getMessagesList().size());
        for (var protoMessage : request.getMessagesList()) {
            messages.add(extendedMessageMapper.fromProto(protoMessage));
        }
        builder.setMessages(messages);

        // Internal states.
        var internalStateMapper = new InternalStateProtoMapper(
                keySchema, codecs.getKeyCodec(), codecs.getInternalStateValueCodec()
        );
        builder.setInternalStates(internalStateMapper.fromProto(request.getInternalStatesList()));

        // External states.
        var externalStateMapper = new ExternalStateProtoMapper(
                keySchema, codecs.getKeyCodec(), codecs.getPayloadCodec()
        );
        builder.setExternalStates(
                externalStateMapper.fromProto(request.getExternalStatesList(), jobId, requestId)
        );

        // Read-only joined external states.
        builder.setJoinedExternalStates(
                externalStateMapper.fromProto(request.getJoinedExternalStatesList(), jobId, requestId)
        );

        // Timers.
        var timerMapper = new TimerProtoMapper(keySchema, codecs.getKeyCodec());
        var timers = new ArrayList<Timer>(request.getTimersList().size());
        for (var protoTimer : request.getTimersList()) {
            timers.add(timerMapper.fromProto(protoTimer));
        }
        builder.setTimers(timers);

        // Visits.
        var visitMapper = new VisitProtoMapper(keySchema, codecs.getKeyCodec());
        var visits = new ArrayList<Visit>(request.getVisitsList().size());
        for (var protoVisit : request.getVisitsList()) {
            visits.add(visitMapper.fromProto(protoVisit));
        }
        builder.setVisits(visits);

        // Watermarks.
        var watermarks = new HashMap<String, Long>();
        long minWatermark = Long.MAX_VALUE;
        for (var protoWatermark : request.getWatermarksList()) {
            long watermark = protoWatermark.getWatermark();
            if (minWatermark > watermark) {
                minWatermark = watermark;
            }
            watermarks.put(protoWatermark.getStreamId(), watermark);
        }
        // SwiftMap / SwiftOrderedSource computations never send watermarks;
        // Fall back to 0 ("no event time has advanced").
        if (watermarks.isEmpty()) {
            minWatermark = 0L;
        }
        builder.setMinWatermark(minWatermark);
        builder.setWatermarks(watermarks);
        return builder.build();
    }
}
