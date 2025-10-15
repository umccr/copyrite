FROM rust:1.90-slim AS builder

LABEL org.opencontainers.image.source=https://github.com/umccr/copyrite
LABEL org.opencontainers.image.url=https://github.com/umccr/copyrite/pkgs/container/copyrite
LABEL org.opencontainers.image.description="CLI tool for efficient checksum and copy operations across object stores"
LABEL org.opencontainers.image.licenses=MIT
LABEL org.opencontainers.image.authors="Marko Malenic <mmalenic1@gmail.com>"

WORKDIR /build

RUN cargo install cargo-strip

COPY . .

RUN cargo build --all-features --release && \
    cargo strip

FROM gcr.io/distroless/cc-debian12

COPY --from=builder /build/target/release/copyrite /usr/local/bin/copyrite

CMD [ "copyrite" ]
