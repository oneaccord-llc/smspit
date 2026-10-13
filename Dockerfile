FROM alpine:3.24 AS prod

WORKDIR /app

RUN apk --update add \
  ca-certificates \
  tzdata \
  --no-cache \
  && update-ca-certificates

ARG UID=10001

COPY ./server /app/server

RUN adduser \
  --disabled-password \
  --gecos "" \
  --home "/nonexistent" \
  --shell "/sbin/nologin" \
  --no-create-home \
  --uid "${UID}" \
  appuser \
  && mkdir /data && chown appuser /data

USER appuser

# Mount the PVC at /data.
ENV SMSPIT_DB=/data/smspit.db
VOLUME /data

EXPOSE 8080

CMD ["/app/server"]
