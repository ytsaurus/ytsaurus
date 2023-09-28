ARG YTSAURUS_TAG=dev
ARG QUERY_TRACKER_TAG=dev
ARG STRAWBERRY_TAG=dev

# Query tracker.
FROM ytsaurus/query-tracker:${QUERY_TRACKER_TAG} as qt
WORKDIR /query-tracker

RUN mkdir -p /query-tracker/usr/bin
RUN mkdir -p /query-tracker/usr/lib

RUN cp /usr/bin/ytserver-all /query-tracker/usr/bin/ytserver-query-tracker
RUN cp /usr/bin/ytserver-yql-agent /query-tracker/usr/bin
RUN cp /usr/bin/mrjob /query-tracker/usr/bin
RUN cp -r /usr/lib/yql /query-tracker/usr/lib
RUN cp /usr/bin/init_query_tracker_state /query-tracker/usr/bin
RUN find /usr/bin -name '*CREDITS' -print0 | xargs -0 -I '{}' cp '{}' /query-tracker/usr/bin

# Strawberry.
FROM ytsaurus/strawberry:${STRAWBERRY_TAG} as strawberry
WORKDIR /strawberry

RUN mkdir -p /strawberry/usr/bin

RUN cp /usr/bin/strawberry-controller /strawberry/usr/bin
RUN find /usr/bin -name '*CREDITS' -print0 | xargs -0 -I '{}' cp '{}' /strawberry/usr/bin

# Main image.
FROM ytsaurus/ytsaurus:${YTSAURUS_TAG}
USER root

COPY --from=qt /query-tracker/usr/bin/* /usr/bin/
COPY --from=qt /query-tracker/usr/lib/* /usr/lib
COPY --from=strawberry /strawberry/usr/bin/* /usr/bin
RUN ln -fs /usr/bin/strawberry-controller /usr/bin/chyt-controller
