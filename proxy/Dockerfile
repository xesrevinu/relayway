FROM busybox:1.36-musl AS busybox

FROM debian:bookworm-slim
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY bin/linux-${TARGETARCH}/relayway-proxy /relayway-proxy
COPY entrypoint.sh /entrypoint.sh
COPY --from=busybox /bin/busybox /bin/busybox
RUN chmod +x /relayway-proxy /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
