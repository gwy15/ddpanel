# build
FROM rust:slim-buster as builder
WORKDIR /code
COPY . .
RUN cargo b --release --no-default-features && strip target/release/ddpanel

# 
FROM debian:buster-slim
WORKDIR /code
COPY --from=builder /code/target/release/ddpanel .
COPY --from=builder /code/log4rs.yml .
ENTRYPOINT [ "./ddpanel" ]
CMD []
