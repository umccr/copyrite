FROM debian

LABEL org.opencontainers.image.source=https://github.com/umccr/copyrite
LABEL org.opencontainers.image.url=https://github.com/umccr/copyrite/pkgs/container/copyrite
LABEL org.opencontainers.image.description="CLI tool for efficient checksum and copy operations across object stores"
LABEL org.opencontainers.image.licenses=MIT
LABEL org.opencontainers.image.authors="Marko Malenic <mmalenic1@gmail.com>"

ARG TARGETOS
ARG GITHUB_REPO_RELEASES_URL
ARG TARGETARCH

WORKDIR /build

# Install debian basics
RUN \
    # Set boolean \
    if [ "${TARGETPLATFORM#linux/}" = "arm64" ]; then \
      platform_url="aarch64";  \
    else \
      platform_url="x86_64"; \
    fi && \
    # Install
    echo "Standard APT" 1>&2 && \
    apt update -yq && \
    apt upgrade -yq && \
    apt install -yq \
      wget && \
    wget \
      --quiet \
      --output-document - \
      "${GITHUB_REPO_RELEASES_URL}/${PACKAGE_VERSION}/copyrite-${platform_url}-linux-gnu.tar.gz" | \
    tar \
      --directory /usr/bin \
      --extract \
      --gzip \
      --file - && \
    chmod +x /usr/bin/copyrite

CMD [ "/usr/bin/copyrite" ]
