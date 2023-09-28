FROM ubuntu:focal

USER root

WORKDIR /tmp

COPY ./chyt-controller /usr/bin/strawberry-controller
RUN ln -s /usr/bin/strawberry-controller /usr/bin/chyt-controller

# CREDITS files with licenses for all binaries.
COPY ./credits/chyt-controller.CREDITS /usr/bin/strawberry-controller.CREDITS
