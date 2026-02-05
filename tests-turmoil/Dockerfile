# Detsim Orchestrator Docker Image
#
# Build:
#   docker build -t openraft-detsim .
#
# Run (local testing):
#   docker run -e DETSIM_DRY_RUN=true openraft-detsim
#
# Run (on GKE with default SA credentials):
#   docker run -e DETSIM_GCS_BUCKET=openraft-detsim-logs openraft-detsim

FROM rustlang/rust:nightly-bookworm AS builder

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY openraft ./openraft
COPY macros ./macros
COPY rt ./rt
COPY rt-tokio ./rt-tokio
COPY rt-monoio ./rt-monoio
COPY rt-compio ./rt-compio
COPY tests-turmoil ./tests-turmoil

# Build release binaries
RUN cd tests-turmoil && cargo build --release --bin fuzz --bin orchestrator --bin fake_fuzz

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    python3 \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

# Install Google Cloud SDK (for gsutil)
RUN curl -sSL https://sdk.cloud.google.com | bash -s -- --disable-prompts --install-dir=/opt
ENV PATH="/opt/google-cloud-sdk/bin:${PATH}"

# Copy binaries
COPY --from=builder /app/tests-turmoil/target/release/fuzz /usr/local/bin/
COPY --from=builder /app/tests-turmoil/target/release/orchestrator /usr/local/bin/
COPY --from=builder /app/tests-turmoil/target/release/fake_fuzz /usr/local/bin/

# Default environment
ENV DETSIM_FUZZ_BINARY=/usr/local/bin/fuzz
ENV DETSIM_MAX_STEPS=100000
ENV DETSIM_ITERATIONS=100

ENTRYPOINT ["/usr/local/bin/orchestrator"]
