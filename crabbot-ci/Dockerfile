ARG BASE_IMAGE=catthehacker/ubuntu:act-latest

FROM ${BASE_IMAGE}

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:${PATH}

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        build-essential \
        ca-certificates \
        curl \
        gcc-mingw-w64-x86-64 \
        pkg-config \
        ripgrep \
        zip \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 --fail --silent --show-error https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain 1.89.0 --profile minimal \
    && rustup component add clippy llvm-tools-preview rustfmt \
    && rustup target add x86_64-pc-windows-gnu \
    && chmod -R a+rX /usr/local/rustup /usr/local/cargo
