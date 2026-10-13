# smspit

Mailpit, but for SMS. Services publish the SMS they would have sent to NATS
JetStream, and smspit stores them in sqlite and shows them in a web inbox.

## Producer side

Instead of calling your SMS provider, publish one JSON message per SMS to the
subject `smspit.sms`:

```json
{
  "campaign": "event-reminder",
  "from": "MGxxxxxxxx",
  "to": "+15551234567",
  "body": "Your event starts in 1 hour."
}
```

| Field      | Notes                                                             |
|------------|-------------------------------------------------------------------|
| `to`       | Recipient. Searchable in the inbox with `to:`.                    |
| `from`     | Sender number or Messaging Service SID. Searchable with `from:`.  |
| `body`     | Message text, shown as-is.                                         |
| `campaign` | Optional label shown as `[campaign]` next to the message.         |

All fields are strings. A message that isn't valid JSON is logged and dropped.

### Rules

- **Publish through JetStream**, not plain NATS publish, so you get an ack
  that the message was stored.
- **smspit owns the stream.** It creates the stream `SMSPIT` (subjects
  `smspit.>`) on startup. Start smspit once before producers publish,
  otherwise the publish fails because no stream exists for the subject.
- **Gate it behind config** so only dev or QA environments publish here, and
  production keeps using the real provider.

### Go example

```go
b, _ := json.Marshal(map[string]string{
	"campaign": "event-reminder",
	"from":     messagingSvcSid,
	"to":       to,
	"body":     message,
})
_, err := js.Publish(ctx, "smspit.sms", b) // js is a jetstream.JetStream
```

In email-service this is already wired up: set `SMS_STRATEGY=smspit`.

### From the CLI

```sh
nats pub smspit.sms '{"to":"+15551234567","from":"MG123","body":"hello"}'
```

## Running smspit

| Flag        | Env               | Default            |
|-------------|-------------------|--------------------|
| `-listen`   |                   | `:8080`            |
| `-nats`     | `NATS_URL`        | `nats://127.0.0.1:4222` |
| `-db`       | `SMSPIT_DB`       | `smspit.db` (`/data/smspit.db` in the image) |
| `-user`     | `SMSPIT_USER`     | `admin`            |
| `-password` | `SMSPIT_PASSWORD` | `Password@123`     |

The inbox is behind HTTP Basic auth. Change the password and serve it over
HTTPS anywhere outside localhost. In Kubernetes, run one replica with the PVC
mounted at `/data`.
