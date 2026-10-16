# syntax=docker/dockerfile:1
# CareHub: builds the open-source AppFlowy-Cloud server and the import worker
# from the source code in this repository. No pre-built AppFlowy images are used.
#
#   docker build -f docker/carehub/server.Dockerfile --target server .
#   docker build -f docker/carehub/server.Dockerfile --target worker .

FROM lukemathwalker/cargo-chef:latest-rust-1.86 AS chef
WORKDIR /app
RUN apt-get update \
  && apt-get install -y --no-install-recommends lld clang protobuf-compiler libprotobuf-dev \
  && rm -rf /var/lib/apt/lists/*

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
# "ci" profile = optimised build without link-time optimisation (much faster to build).
ARG CARGO_PROFILE=ci
ENV CARGO_BUILD_JOBS=2 \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
COPY --from=planner /app/recipe.json recipe.json
# CareHub: the patched collaboration library (prepared by the build workflow,
# see carehub/collab/apply.py) must be present before dependencies are built.
COPY vendor vendor
RUN cargo chef cook --profile ${CARGO_PROFILE} --recipe-path recipe.json
COPY . .
ENV SQLX_OFFLINE=true
RUN cargo build --profile ${CARGO_PROFILE} --bin appflowy_cloud \
  && cd services/appflowy-worker \
  && cargo build --profile ${CARGO_PROFILE} --bin appflowy_worker \
  && mkdir -p /out \
  && cp /app/target/${CARGO_PROFILE}/appflowy_cloud /app/target/${CARGO_PROFILE}/appflowy_worker /out/

FROM debian:bookworm-slim AS runtime
RUN apt-get update -y \
  && apt-get install -y --no-install-recommends openssl ca-certificates curl \
  && update-ca-certificates \
  && apt-get clean -y \
  && rm -rf /var/lib/apt/lists/*
ENV APP_ENVIRONMENT=production \
    RUST_BACKTRACE=1
WORKDIR /app

FROM runtime AS worker
COPY --from=builder /out/appflowy_worker /usr/local/bin/appflowy_worker
CMD ["appflowy_worker"]

FROM runtime AS server
COPY --from=builder /out/appflowy_cloud /usr/local/bin/appflowy_cloud
ENV PORT=8000
EXPOSE 8000
CMD ["appflowy_cloud"]

