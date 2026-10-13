// smspit: Mailpit, but for SMS. Apps publish their outgoing SMS to the NATS
// JetStream subject smspit.sms and you read them in a browser instead of on a phone.
package main

import (
	"cmp"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"flag"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	_ "modernc.org/sqlite"
)

type SMS struct {
	Campaign string
	From     string
	To       string
	Body     string
	At       time.Time
}

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // ponytail: one writer, one reader; sqlite doesn't need more here.
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS sms (
		id INTEGER PRIMARY KEY, campaign TEXT, sender TEXT, recipient TEXT, body TEXT, at INTEGER)`)
	return db, err
}

// list returns the newest messages matching q, a space-separated search where
// to:X and from:X filter on recipient/sender and any other word must appear in
// the body. All matches are case-insensitive substrings, all terms must match.
func list(db *sql.DB, q string) ([]SMS, error) {
	where, args := "1", []any{}
	// ponytail: no quoted phrases, every word is its own term. Add if anyone searches "two words".
	for _, term := range strings.Fields(q) {
		col := "body"
		if v, ok := strings.CutPrefix(term, "to:"); ok {
			col, term = "recipient", v
		} else if v, ok := strings.CutPrefix(term, "from:"); ok {
			col, term = "sender", v
		}
		where += " AND instr(lower(" + col + "), lower(?)) > 0"
		args = append(args, term)
	}
	// ponytail: newest 500 only, the page reloads every 3s. Paging if anyone needs older.
	rows, err := db.Query(`SELECT campaign, sender, recipient, body, at FROM sms WHERE `+where+` ORDER BY id DESC LIMIT 500`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SMS
	for rows.Next() {
		var m SMS
		var at int64
		if err := rows.Scan(&m.Campaign, &m.From, &m.To, &m.Body, &at); err != nil {
			return nil, err
		}
		m.At = time.UnixMilli(at)
		out = append(out, m)
	}
	return out, rows.Err()
}

var page = template.Must(template.New("").Parse(`<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>smspit</title>
<style>
body{font-family:system-ui,sans-serif;max-width:720px;margin:2rem auto;padding:0 1rem;background:#f4f4f5;color:#18181b}
header{display:flex;justify-content:space-between;align-items:center}
input[type=search]{width:100%;box-sizing:border-box;padding:.5rem .75rem;font:inherit;border:1px solid #d4d4d8;border-radius:8px;background:inherit;color:inherit}
.sms{background:#fff;border-radius:12px;padding:.75rem 1rem;margin:.75rem 0;box-shadow:0 1px 2px #0002}
.meta{font-size:.8rem;color:#71717a;display:flex;justify-content:space-between}
.body{white-space:pre-wrap;margin-top:.4rem;word-break:break-word}
@media (prefers-color-scheme:dark){body{background:#18181b;color:#f4f4f5}.sms{background:#27272a}}
</style></head><body>
<header><h1>smspit <small>({{len .Msgs}})</small></h1>
<form method="post" action="/clear"><button>Clear all</button></form></header>
<form><input type="search" name="q" value="{{.Q}}" placeholder="to:+1555 from:MG reminder" aria-label="Search messages"></form>
{{range .Msgs}}<div class="sms">
<div class="meta"><span>{{with .Campaign}}[{{.}}] {{end}}{{.From}} → <b>{{.To}}</b></span><span>{{.At.Format "15:04:05 Jan 2"}}</span></div>
<div class="body">{{.Body}}</div></div>
{{else}}{{if .Q}}<p>No messages match.</p>{{else}}<p>No messages yet. Publish JSON <code>{"campaign","from","to","body"}</code> to <code>smspit.sms</code>.</p>{{end}}{{end}}
<script>setInterval(() => document.activeElement.name !== "q" && location.reload(), 3000)</script>
</body></html>`))

// basicAuth guards h with HTTP Basic auth. Comparing sha256 digests keeps the
// check constant-time regardless of input length.
func basicAuth(h http.Handler, user, pass string) http.Handler {
	wantU, wantP := sha256.Sum256([]byte(user)), sha256.Sum256([]byte(pass))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, _ := r.BasicAuth()
		gotU, gotP := sha256.Sum256([]byte(u)), sha256.Sum256([]byte(p))
		if subtle.ConstantTimeCompare(gotU[:], wantU[:])&subtle.ConstantTimeCompare(gotP[:], wantP[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="smspit", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func routes(db *sql.DB) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		q := r.FormValue("q")
		msgs, err := list(db, q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		page.Execute(w, struct {
			Q    string
			Msgs []SMS
		}{q, msgs})
	})
	mux.HandleFunc("POST /clear", func(w http.ResponseWriter, r *http.Request) {
		if _, err := db.Exec(`DELETE FROM sms`); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/", http.StatusSeeOther)
	})
	return mux
}

// consume moves messages from the SMSPIT stream into db. The durable consumer
// resumes where it left off; a message is acked only once it's in sqlite.
// The returned stop waits for the in-flight message, then disconnects.
func consume(ctx context.Context, natsURL string, db *sql.DB) (stop func(), err error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "SMSPIT", Subjects: []string{"smspit.>"}, MaxAge: 7 * 24 * time.Hour,
	}); err != nil {
		return nil, err
	}
	c, err := js.CreateOrUpdateConsumer(ctx, "SMSPIT", jetstream.ConsumerConfig{
		Durable: "smspit", AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, err
	}
	cc, err := c.Consume(func(m jetstream.Msg) {
		var sms SMS
		if err := json.Unmarshal(m.Data(), &sms); err != nil {
			log.Printf("dropping bad message on %s: %v", m.Subject(), err)
			m.Term()
			return
		}
		at := time.Now()
		if md, err := m.Metadata(); err == nil {
			at = md.Timestamp
		}
		// ponytail: a crash between insert and ack redelivers, so a rare duplicate row is possible.
		if _, err := db.Exec(`INSERT INTO sms (campaign, sender, recipient, body, at) VALUES (?, ?, ?, ?, ?)`,
			sms.Campaign, sms.From, sms.To, sms.Body, at.UnixMilli()); err != nil {
			log.Printf("insert: %v", err)
			m.Nak()
			return
		}
		m.Ack()
	})
	if err != nil {
		return nil, err
	}
	return func() {
		cc.Drain()
		<-cc.Closed()
		nc.Close()
	}, nil
}

const defaultPassword = "password"

func main() {
	addr := flag.String("listen", ":8080", "address to listen on")
	natsURL := flag.String("nats", cmp.Or(os.Getenv("NATS_URL"), nats.DefaultURL), "NATS server url")
	dbPath := flag.String("db", cmp.Or(os.Getenv("SMSPIT_DB"), "smspit.db"), "sqlite file (put it on the PVC)")
	user := flag.String("user", cmp.Or(os.Getenv("SMSPIT_USER"), "admin"), "basic auth username")
	pass := flag.String("password", cmp.Or(os.Getenv("SMSPIT_PASSWORD"), defaultPassword), "basic auth password")
	flag.Parse()
	if *pass == defaultPassword {
		log.Print("warning: using the default password, set SMSPIT_PASSWORD or -password")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := openDB(*dbPath)
	if err != nil {
		log.Fatalf("sqlite: %v", err)
	}

	stopConsume, err := consume(ctx, *natsURL, db)
	if err != nil {
		log.Fatalf("nats: %v", err)
	}

	srv := &http.Server{
		Addr:    *addr,
		Handler: http.NewCrossOriginProtection().Handler(basicAuth(routes(db), *user, *pass)),
	}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	log.Printf("smspit listening on %s, consuming smspit.> from %s into %s", *addr, *natsURL, *dbPath)

	<-ctx.Done()
	stop() // a second Ctrl+C kills immediately
	log.Print("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
	stopConsume()
	db.Close()
}
