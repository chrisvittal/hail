FROM rust:alpine AS builder

RUN apk add --no-cache libc-dev
COPY Cargo.lock Cargo.toml /rssvc/
COPY src /rssvc/src
RUN cargo install --locked --path /rssvc/

FROM alpine
COPY --from=builder /usr/local/cargo/bin/hail-hello-service /rssvc
ENV RUST_LOG=info
EXPOSE 5000

CMD ["/rssvc"]
