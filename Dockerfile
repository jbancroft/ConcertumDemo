FROM golang:1.27.3 AS build

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/demo ./cmd/memnode \
    && mkdir /data

FROM scratch

COPY --from=build /out/ /bin/
COPY --from=build --chown=65532:65532 /data/ /data/
USER 65532:65532

CMD ["/bin/demo", "-memnode-bin", "/bin/memnode", "-data-dir", "/data"]
