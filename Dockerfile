# syntax=docker/dockerfile:1
ARG versionInfo=1.00.000

# ---- build stage --------------------------------------------------------------------------
FROM rust:1.93-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake build-essential && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
# warm the dependency cache (librdkafka is built from source via cmake)
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs && cargo build --release && rm -rf src
COPY . .
RUN touch src/main.rs src/lib.rs && cargo build --release

# ---- runtime stage -------------------------------------------------------------------------
FROM debian:bookworm-slim
ARG versionInfo
ENV DOCKER_APP_VERSION=${versionInfo}
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/target/release/uas /app/uas
COPY src/version.json /app/version.json
EXPOSE 7073
CMD ["/app/uas"]
