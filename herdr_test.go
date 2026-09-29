package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	ext "github.com/mark3labs/kit/extensions"
	"github.com/mark3labs/kit/pkg/extensions/test"
)

type herdrCall struct {
	ID     string
	Method string
	Params map[string]interface{}
}

func (c herdrCall) state() string {
	s, _ := c.Params["state"].(string)
	return s
}

func (c herdrCall) seq() int64 {
	switch v := c.Params["seq"].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	}
	return 0
}

type server struct {
	t    *testing.T
	ln   net.Listener
	path string

	mu     sync.Mutex
	calls  []herdrCall
	conns  int
	live   []net.Conn
	closed bool

	replyErr string

	silent bool

	closeAfterReply bool

	done chan struct{}
}

func newServer(t *testing.T, dir string) *server {
	t.Helper()
	path := filepath.Join(dir, "h.sock")
	srv := listenOn(t, dir, path)
	return srv
}

func listenOn(t *testing.T, dir, path string) *server {
	t.Helper()

	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen %s: %v", path, err)
	}
	s := &server{t: t, ln: ln, path: path, done: make(chan struct{})}
	go s.serve()
	t.Cleanup(s.Close)
	return s
}

func (s *server) serve() {
	defer close(s.done)
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns++
		s.live = append(s.live, conn)
		s.mu.Unlock()
		go s.handle(conn)
	}
}

func (s *server) killAll() {
	s.mu.Lock()
	live := s.live
	s.live = nil
	s.mu.Unlock()
	for _, c := range live {
		_ = c.Close()
	}
}

func (s *server) handle(conn net.Conn) {
	defer conn.Close()
	dec := json.NewDecoder(conn)
	for {
		var raw struct {
			ID     string                 `json:"id"`
			Method string                 `json:"method"`
			Params map[string]interface{} `json:"params"`
		}
		if err := dec.Decode(&raw); err != nil {
			return
		}

		s.mu.Lock()
		s.calls = append(s.calls, herdrCall{ID: raw.ID, Method: raw.Method, Params: raw.Params})
		errCode := s.replyErr
		silent := s.silent
		closeAfter := s.closeAfterReply
		s.mu.Unlock()

		if silent {
			continue
		}

		var resp map[string]interface{}
		if errCode != "" {
			resp = map[string]interface{}{
				"id":    raw.ID,
				"error": map[string]interface{}{"code": errCode, "message": "synthetic"},
			}
		} else {
			resp = map[string]interface{}{
				"id":     raw.ID,
				"result": map[string]interface{}{"type": "ok"},
			}
		}
		line, err := json.Marshal(resp)
		if err != nil {
			return
		}
		if _, err := conn.Write(append(line, '\n')); err != nil {
			return
		}
		if closeAfter {
			return
		}
	}
}

func (s *server) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	_ = s.ln.Close()
	<-s.done
}

func (s *server) setReplyErr(code string) {
	s.mu.Lock()
	s.replyErr = code
	s.mu.Unlock()
}

func (s *server) setSilent(v bool) {
	s.mu.Lock()
	s.silent = v
	s.mu.Unlock()
}

func (s *server) setCloseAfterReply(v bool) {
	s.mu.Lock()
	s.closeAfterReply = v
	s.mu.Unlock()
}

func (s *server) waitFor(t *testing.T, n int) []herdrCall {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if calls := s.snapshot(); len(calls) >= n {
			return calls
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d herdr requests, got %d: %v",
		n, len(s.snapshot()), s.snapshot())
	return nil
}

func (s *server) snapshot() []herdrCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]herdrCall, len(s.calls))
	copy(out, s.calls)
	return out
}

func (s *server) states() []string {
	var out []string
	for _, c := range s.stateReports() {
		out = append(out, c.state())
	}
	return out
}

func (s *server) lastState() string {
	reports := s.stateReports()
	if len(reports) == 0 {
		return ""
	}
	return reports[len(reports)-1].state()
}

func (c herdrCall) idleLabel() string {
	labels, _ := c.Params["state_labels"].(map[string]interface{})
	if labels == nil {
		return ""
	}
	v, _ := labels["idle"].(string)
	return v
}

func (c herdrCall) has(name string) bool {
	_, ok := c.Params[name]
	return ok
}

func (s *server) methods() []string {
	var out []string
	for _, c := range s.snapshot() {
		out = append(out, c.Method)
	}
	return out
}

func (s *server) seqs() []int64 {
	var out []int64
	for _, c := range s.snapshot() {
		out = append(out, c.seq())
	}
	return out
}

func (c herdrCall) param(name string) string {
	s, _ := c.Params[name].(string)
	return s
}

func emit(r *ext.Runner, e ext.Event) {
	if _, err := r.Emit(e); err != nil {
		panic(fmt.Sprintf("emit %T: %v", e, err))
	}
}

func loadHerdrBusy(t *testing.T) *ext.Runner {
	t.Helper()

	loaded, err := ext.LoadExplicitExtensions([]string{"./herdr.go"})
	if err != nil {
		t.Fatalf("load herdr.go: %v", err)
	}
	runner := ext.NewRunner(loaded)
	mc := test.NewMockContext()
	mc.Interactive = true
	ctx := mc.ToContext()
	ctx.IsIdle = func() bool { return false }
	runner.SetContext(ctx)
	return runner
}

func loadHerdr(t *testing.T) *ext.Runner {
	t.Helper()
	r, _ := loadHerdrMock(t)
	return r
}

func loadHerdrMock(t *testing.T) (*ext.Runner, *test.MockContext) {
	t.Helper()

	loaded, err := ext.LoadExplicitExtensions([]string{"./herdr.go"})
	if err != nil {
		t.Fatalf("load herdr.go: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 extension loaded, got %d", len(loaded))
	}

	runner := ext.NewRunner(loaded)
	mc := test.NewMockContext()
	mc.Interactive = true
	runner.SetContext(mc.ToContext())
	return runner, mc
}

func herdrEnv(t *testing.T, srv *server, pane string) {
	t.Helper()
	t.Setenv("HERDR_ENV", "1")
	t.Setenv("HERDR_SOCKET_PATH", srv.path)
	t.Setenv("HERDR_PANE_ID", pane)
	t.Setenv("HERDR_BIN_PATH", "/nonexistent/herdr")
}

const settleWindow = 2 * time.Second

func waitSettled() { time.Sleep(settleWindow) }

func TestHerdr_InactiveOutsideHerdr(t *testing.T) {
	t.Setenv("HERDR_ENV", "")
	t.Setenv("HERDR_SOCKET_PATH", "")
	t.Setenv("HERDR_PANE_ID", "")

	r := loadHerdr(t)
	if got := len(r.Extensions()[0].Handlers); got != 0 {
		t.Fatalf("expected 0 handlers outside Herdr, got %d", got)
	}
}

func TestHerdr_RequiresHerdrEnvFlag(t *testing.T) {
	srv := newServer(t, t.TempDir())

	t.Setenv("HERDR_ENV", "")
	t.Setenv("HERDR_SOCKET_PATH", srv.path)
	t.Setenv("HERDR_PANE_ID", "w1:p1")

	r := loadHerdr(t)
	if got := len(r.Extensions()[0].Handlers); got != 0 {
		t.Fatalf("expected 0 handlers without HERDR_ENV, got %d", got)
	}

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	time.Sleep(200 * time.Millisecond)
	if calls := srv.snapshot(); len(calls) != 0 {
		t.Fatalf("extension reported %d request(s) with HERDR_ENV unset: %v", len(calls), calls)
	}
}

func TestHerdr_RequiresSocketAndPane(t *testing.T) {
	t.Run("missing socket", func(t *testing.T) {
		t.Setenv("HERDR_ENV", "1")
		t.Setenv("HERDR_SOCKET_PATH", "")
		t.Setenv("HERDR_PANE_ID", "w1:p1")

		r := loadHerdr(t)
		if got := len(r.Extensions()[0].Handlers); got != 0 {
			t.Fatalf("expected 0 handlers without a socket path, got %d", got)
		}
	})

	t.Run("missing pane", func(t *testing.T) {
		srv := newServer(t, t.TempDir())
		t.Setenv("HERDR_ENV", "1")
		t.Setenv("HERDR_SOCKET_PATH", srv.path)
		t.Setenv("HERDR_PANE_ID", "")

		r := loadHerdr(t)
		if got := len(r.Extensions()[0].Handlers); got != 0 {
			t.Fatalf("expected 0 handlers without a pane id, got %d", got)
		}
		emit(r, ext.TurnStateChangeEvent{State: "working"})
		time.Sleep(200 * time.Millisecond)
		if calls := srv.snapshot(); len(calls) != 0 {
			t.Fatalf("reported %d request(s) without a pane id: %v", len(calls), calls)
		}
	})
}

func TestHerdr_RegistersHandlersInsideHerdr(t *testing.T) {
	herdrEnv(t, newServer(t, t.TempDir()), "w1:p1")

	r := loadHerdr(t)
	if got := len(r.Extensions()[0].Handlers); got == 0 {
		t.Fatal("expected handlers to be registered inside a Herdr pane")
	}
}

func TestHerdr_SeqIsStrictlyIncreasing(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.SessionStartEvent{})
	emit(r, ext.TurnStateChangeEvent{State: "working"})
	emit(r, ext.PromptStateChangeEvent{State: "open"})
	srv.waitFor(t, 3)

	seqs := srv.seqs()
	if len(seqs) < 3 {
		t.Fatalf("expected at least 3 reports, got %d", len(seqs))
	}
	for i, s := range seqs {
		if s <= 0 {
			t.Fatalf("report %d carried seq %d, want a positive value", i, s)
		}
		if i > 0 && s <= seqs[i-1] {
			t.Fatalf("seq not strictly increasing at %d: %v", i, seqs)
		}
	}
}

func TestHerdr_SeqStartsAboveTheClock(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.SessionStartEvent{})
	srv.waitFor(t, 1)

	first := srv.seqs()[0]
	if first < 1_000_000_000_000 {
		t.Fatalf("first seq = %d, want a clock-based value (>=1e12) so it is "+
			"never stale against a previous session's reports", first)
	}
}

func TestHerdr_ReportsWorkingThenIdle(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.SessionStartEvent{})
	srv.waitFor(t, 1)
	if got := srv.lastState(); got != "idle" {
		t.Fatalf("session start state = %q, want idle", got)
	}

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 2)
	if got := srv.lastState(); got != "working" {
		t.Fatalf("turn working state = %q, want working", got)
	}
}

func TestHerdr_UsesStableSourceAndAgent(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	calls := srv.waitFor(t, 1)
	c := calls[0]

	if c.Method != "pane.report_agent" {
		t.Fatalf("method = %q, want pane.report_agent", c.Method)
	}
	if got := c.param("source"); got != "custom:kit" {
		t.Fatalf("source = %q, want custom:kit", got)
	}
	if got := c.param("agent"); got != "kit" {
		t.Fatalf("agent = %q, want kit", got)
	}
	if got := c.param("pane_id"); got != "w1:p1" {
		t.Fatalf("pane_id = %q, want w1:p1", got)
	}
	if c.ID == "" {
		t.Fatalf("request carried no id: %+v", c)
	}
}

func TestHerdr_BlockedOnModalOpen(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	emit(r, ext.PromptStateChangeEvent{Kind: "prompt", State: "open"})
	calls := srv.waitFor(t, 2)
	last := calls[len(calls)-1]

	if got := last.state(); got != "blocked" {
		t.Fatalf("modal open state = %q, want blocked", got)
	}
	if last.param("message") == "" {
		t.Fatalf("blocked report carried no message: %+v", last)
	}
}

func TestHerdr_ModalCloseDoesNotInventATurnEnd(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.PromptStateChangeEvent{State: "open"})
	srv.waitFor(t, 1)
	before := len(srv.snapshot())

	emit(r, ext.PromptStateChangeEvent{State: "closed"})
	time.Sleep(200 * time.Millisecond)

	if after := len(srv.snapshot()); after != before {
		t.Fatalf("close edge reported %d extra call(s), want 0", after-before)
	}
}

func TestHerdr_BlockedWhenTurnStillRunningAtSettle(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdrBusy(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)
	emit(r, ext.AgentEndEvent{StopReason: "completed"})

	waitSettled()
	if got := srv.lastState(); got != "blocked" {
		t.Fatalf("state after a still-busy settle = %q, want blocked", got)
	}
}

func TestHerdr_QuestionResponseIsBlocked(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	emit(r, ext.MessageEndEvent{Content: "Renamed the file. Which name do you want?"})
	emit(r, ext.AgentEndEvent{StopReason: "completed"})
	waitSettled()

	if got := srv.lastState(); got != "blocked" {
		t.Fatalf("state after a question-ending turn = %q, want blocked", got)
	}
}

func TestHerdr_NonQuestionResponseIsIdle(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	emit(r, ext.MessageEndEvent{Content: "All green, applied the patch and ran the tests."})
	emit(r, ext.AgentEndEvent{StopReason: "completed"})
	waitSettled()

	if got := srv.lastState(); got != "idle" {
		t.Fatalf("state after a clean turn = %q, want idle", got)
	}
}

func TestHerdr_CancelledTurnSettlesToIdle(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	emit(r, ext.ErrorEvent{Error: "context canceled"})
	emit(r, ext.AgentEndEvent{StopReason: "error"})
	waitSettled()

	if got := srv.lastState(); got != "idle" {
		t.Fatalf("state after a cancelled turn = %q, want idle", got)
	}
}

func TestHerdr_ToolEndDoesNotClobberTerminalVerdict(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	emit(r, ext.MessageEndEvent{Content: "Done."})
	emit(r, ext.AgentEndEvent{StopReason: "completed"})
	waitSettled()
	if got := srv.lastState(); got != "idle" {
		t.Fatalf("precondition: want idle, got %q", got)
	}

	emit(r, ext.ToolExecutionEndEvent{ToolName: "bash"})
	time.Sleep(200 * time.Millisecond)
	if got := srv.lastState(); got != "idle" {
		t.Fatalf("straggler clobbered verdict: state = %q, want idle", got)
	}
}

func TestHerdr_IdenticalConsecutiveStatesAreDeduped(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)
	before := len(srv.snapshot())

	for i := 0; i < 5; i++ {
		emit(r, ext.ToolExecutionEndEvent{ToolName: "bash"})
	}
	time.Sleep(200 * time.Millisecond)

	if after := len(srv.snapshot()); after != before {
		t.Fatalf("dedup failed: %d extra call(s) for identical state", after-before)
	}
}

func TestHerdr_ShutdownReleasesAuthority(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	emit(r, ext.SessionShutdownEvent{})

	deadline := time.Now().Add(5 * time.Second)
	var calls []herdrCall
	for time.Now().Before(deadline) {
		calls = srv.snapshot()
		for _, c := range calls {
			if c.Method == "pane.release_agent" {
				goto found
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no pane.release_agent request recorded; got %v", srv.methods())

found:
	last := calls[len(calls)-1]
	if last.Method != "pane.release_agent" {
		t.Fatalf("last request was %q, want pane.release_agent", last.Method)
	}
	if got := last.param("source"); got != "custom:kit" {
		t.Fatalf("release source = %q, want custom:kit", got)
	}
	if got := last.param("agent"); got != "kit" {
		t.Fatalf("release agent = %q, want kit", got)
	}
	if got := last.param("pane_id"); got != "w1:p1" {
		t.Fatalf("release pane_id = %q, want w1:p1", got)
	}
}

func TestHerdr_NoReportsAfterRelease(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.SessionShutdownEvent{})
	srv.waitFor(t, 1)
	before := len(srv.snapshot())

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	emit(r, ext.AgentStartEvent{})
	time.Sleep(300 * time.Millisecond)

	for _, c := range srv.snapshot()[before:] {
		if c.Method == "pane.report_agent" {
			t.Fatalf("pane.report_agent issued after release: %+v", c)
		}
	}
}

func TestHerdr_ConstantsMatchExtension(t *testing.T) {
	src, err := os.ReadFile("./herdr.go")
	if err != nil {
		t.Fatalf("read herdr.go: %v", err)
	}

	for _, want := range []struct {
		pattern string
		what    string
	}{
		{`herdrSource\s*=\s*"custom:kit"`, "herdrSource"},
		{`herdrAgent\s*=\s*"kit"`, "herdrAgent"},
		{`herdrWorking\s*=\s*"working"`, "herdrWorking"},
		{`herdrIdle\s*=\s*"idle"`, "herdrIdle"},
		{`herdrBlocked\s*=\s*"blocked"`, "herdrBlocked"},
		{`const settleDelay = 400 \* time\.Millisecond`, "settleDelay"},
		{`"pane\.report_agent"`, "pane.report_agent"},
		{`"pane\.release_agent"`, "pane.release_agent"},
	} {
		re, err := regexp.Compile(want.pattern)
		if err != nil {
			t.Fatalf("bad pattern for %s: %v", want.what, err)
		}
		if !re.Match(src) {
			t.Errorf("herdr.go no longer declares %s (pattern %q)", want.what, want.pattern)
		}
	}
}

func TestHerdr_QuestionHeuristic(t *testing.T) {
	cases := []struct {
		name      string
		in        string
		wantBlock bool
	}{
		{"closes with a real question", "Renamed it. Which name do you want?", true},
		{"leading wh-word", "What changed in the parser?", true},
		{"bare declarative with a tag", "All green, right?", false},
		{"question answered mid-message", "It costs $2. Why? Anyway, here is the patch.", false},
		{"plain statement", "Applied the patch and ran the tests.", false},
		{"glob inside fenced code", "Here you go:\n```\nls *.*\n```", false},
		{"list item asking", "Options:\n- keep both\n- drop one\n\nWhich one?", true},
		{"fullwidth question", "下一步怎么做？", true},
		{"empty", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newServer(t, t.TempDir())
			herdrEnv(t, srv, "w1:p1")
			r := loadHerdr(t)

			emit(r, ext.TurnStateChangeEvent{State: "working"})
			srv.waitFor(t, 1)
			emit(r, ext.MessageEndEvent{Content: tc.in})
			emit(r, ext.AgentEndEvent{StopReason: "completed"})
			waitSettled()

			want := "idle"
			if tc.wantBlock {
				want = "blocked"
			}
			if got := srv.lastState(); got != want {
				t.Fatalf("content %q: state = %q, want %q; all states = %v",
					tc.in, got, want, srv.states())
			}
		})
	}
}

func TestHerdr_CancelClassification(t *testing.T) {
	cancelMarkers := []string{
		"context canceled",
		"operation was cancelled",
		"request canceled",
		"err_canceled",
		"CONTEXT CANCELED",
	}
	for _, msg := range cancelMarkers {
		t.Run("cancel/"+msg, func(t *testing.T) {
			srv := newServer(t, t.TempDir())
			herdrEnv(t, srv, "w1:p1")
			r := loadHerdr(t)

			emit(r, ext.TurnStateChangeEvent{State: "working"})
			srv.waitFor(t, 1)
			emit(r, ext.ErrorEvent{Error: msg})
			emit(r, ext.AgentEndEvent{StopReason: "error"})
			waitSettled()

			if got := srv.lastState(); got != "idle" {
				t.Fatalf("cancelled turn settled to %q, want idle", got)
			}
		})
	}

	t.Run("genuine error mid-turn keeps the turn working", func(t *testing.T) {
		srv := newServer(t, t.TempDir())
		herdrEnv(t, srv, "w1:p1")
		r := loadHerdrBusy(t)

		emit(r, ext.TurnStateChangeEvent{State: "working"})
		srv.waitFor(t, 1)
		emit(r, ext.ErrorEvent{Error: "connection reset by peer"})
		waitSettled()

		if got := srv.lastState(); got != "working" {
			t.Fatalf("busy agent after a mid-turn error = %q, want working", got)
		}
		if got := srv.lastIdleLabel(); got != "" && got != "retrying" {
			t.Fatalf("label after a mid-turn error = %q, want retrying or unset", got)
		}
	})
}

func TestHerdr_DedupForwardsRealTransitions(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)
	emit(r, ext.AgentEndEvent{StopReason: "completed"})
	waitSettled()
	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 3)

	states := srv.states()
	want := []string{"working", "idle", "working"}
	if len(states) < len(want) {
		t.Fatalf("got %v, want at least %v", states, want)
	}
	for i, w := range want {
		if states[i] != w {
			t.Fatalf("states = %v, want prefix %v", states, want)
		}
	}

	before := len(srv.snapshot())
	emit(r, ext.PromptStateChangeEvent{Kind: "prompt", State: "open"})
	srv.waitFor(t, 4)
	if len(srv.snapshot()) <= before {
		t.Fatal("a block report was suppressed by de-duplication")
	}
}

func TestHerdr_ReportRequestShape(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w9:p3")
	r := loadHerdr(t)

	emit(r, ext.PromptStateChangeEvent{Kind: "prompt", State: "open"})
	calls := srv.waitFor(t, 1)
	got := calls[0]

	if got.Method != "pane.report_agent" {
		t.Fatalf("method = %q, want pane.report_agent", got.Method)
	}
	for key, want := range map[string]string{
		"pane_id": "w9:p3",
		"source":  "custom:kit",
		"agent":   "kit",
		"state":   "blocked",
	} {
		if v := got.param(key); v != want {
			t.Errorf("params[%q] = %q, want %q", key, v, want)
		}
	}
	if got.seq() == 0 {
		t.Errorf("missing seq: %+v", got)
	}
	if got.param("message") == "" {
		t.Errorf("blocked report carried no message: %+v", got)
	}
}

func TestHerdr_NoMessageWhenNotBlocked(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	calls := srv.waitFor(t, 1)

	if _, ok := calls[0].Params["message"]; ok {
		t.Fatalf("working report carried a message param: %+v", calls[0])
	}
}

func TestHerdr_SourceIsValidPerHerdrRules(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	emit(r, ext.SessionShutdownEvent{})
	deadline := time.Now().Add(3 * time.Second)
	var all []herdrCall
	for time.Now().Before(deadline) {
		all = srv.snapshot()
		if hasRelease(all) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if len(all) == 0 {
		t.Fatal("no calls recorded")
	}
	valid := func(s string) bool {
		if s == "" || len(s) > 80 {
			return false
		}
		for _, r := range s {
			ok := r == ':' || r == '.' || r == '_' || r == '-' ||
				(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
			if !ok {
				return false
			}
		}
		return true
	}
	for _, c := range all {
		src := c.param("source")
		if !valid(src) {
			t.Errorf("invalid source %q in %s: %+v", src, c.Method, c)
		}
		if got := c.param("agent"); got != "kit" {
			t.Errorf("agent = %q, want kit", got)
		}
	}
}

func hasRelease(calls []herdrCall) bool {
	for _, c := range calls {
		if c.Method == "pane.release_agent" {
			return true
		}
	}
	return false
}

func TestHerdr_OnlyKnownStatesAreReported(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.SessionStartEvent{})
	emit(r, ext.TurnStateChangeEvent{State: "working"})
	emit(r, ext.AgentStartEvent{})
	emit(r, ext.ToolExecutionStartEvent{ToolName: "bash"})
	emit(r, ext.RetryEvent{Attempt: 2})
	emit(r, ext.SubagentStartEvent{ToolCallID: "t1"})
	emit(r, ext.ErrorEvent{Error: "context canceled"})
	emit(r, ext.AgentEndEvent{StopReason: "cancelled"})
	emit(r, ext.PromptStateChangeEvent{State: "open"})
	srv.waitFor(t, 3)
	waitSettled()

	valid := map[string]bool{"working": true, "idle": true, "blocked": true}
	for _, s := range srv.states() {
		if !valid[s] {
			t.Errorf("reported unknown state %q", s)
		}
	}
}

func (s *server) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns
}

func TestHerdr_SurvivesServerClosingEachConnection(t *testing.T) {
	srv := newServer(t, t.TempDir())
	srv.setCloseAfterReply(true)
	herdrEnv(t, srv, "w1:p1")
	r, mc := loadHerdrMock(t)

	emit(r, ext.SessionStartEvent{})                    // idle
	emit(r, ext.TurnStateChangeEvent{State: "working"}) // working
	emit(r, ext.PromptStateChangeEvent{State: "open"})  // blocked
	emit(r, ext.AgentEndEvent{StopReason: "completed"}) // settles idle
	waitSettled()

	if len(mc.PrintErrors) != 0 {
		t.Errorf("server closes every connection, but %d error(s) were reported: %v",
			len(mc.PrintErrors), mc.PrintErrors)
	}
	delivered := srv.snapshot()
	if len(delivered) < 4 {
		t.Fatalf("only %d of 4 reports delivered: %v", len(delivered), srv.states())
	}
	want := []string{"idle", "working", "blocked", "blocked"}
	for i, w := range want {
		if i < len(delivered) && delivered[i].state() != w {
			t.Errorf("report %d = %q, want %q (all: %v)", i, delivered[i].state(), w, srv.states())
		}
	}
}

func TestHerdr_StopsReusingAfterServerCloses(t *testing.T) {
	srv := newServer(t, t.TempDir())
	srv.setCloseAfterReply(true)
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.SessionStartEvent{})
	emit(r, ext.TurnStateChangeEvent{State: "working"})
	emit(r, ext.PromptStateChangeEvent{State: "open"})
	emit(r, ext.PromptStateChangeEvent{State: "closed"})
	emit(r, ext.TurnStateChangeEvent{State: "idle"})
	waitSettled()

	conns := srv.connCount()
	reports := len(srv.snapshot())
	if conns < reports {
		t.Fatalf("only %d connections for %d reports: still reusing a connection "+
			"the server closes, so reports are being lost", conns, reports)
	}
}

func TestHerdr_RetryUsesAHigherSeq(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	srv.killAll()

	emit(r, ext.PromptStateChangeEvent{State: "open"})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		calls := srv.snapshot()
		if len(calls) >= 2 {
			first, second := calls[len(calls)-2], calls[len(calls)-1]
			if second.seq() <= first.seq() {
				t.Fatalf("retry reused seq %d after %d; Herdr would drop it: %v",
					second.seq(), first.seq(), calls)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no retry observed after a stale connection: %v", srv.seqs())
}

func TestHerdr_ReusesOneConnection(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.SessionStartEvent{})                     // idle
	emit(r, ext.TurnStateChangeEvent{State: "working"})  // working
	emit(r, ext.PromptStateChangeEvent{State: "open"})   // blocked
	emit(r, ext.PromptStateChangeEvent{State: "closed"}) // no report
	emit(r, ext.AgentEndEvent{StopReason: "completed"})  // settles idle
	waitSettled()
	srv.waitFor(t, 3)

	if n := srv.connCount(); n != 1 {
		t.Fatalf("used %d connections for %d reports, want exactly 1 (no reconnect)",
			n, len(srv.snapshot()))
	}
}

func TestHerdr_RecoversFromDeadServer(t *testing.T) {
	dir := t.TempDir()
	srv := newServer(t, dir)
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	srv.Close()
	srv.killAll()

	restarted := listenOn(t, dir, srv.path)
	defer restarted.Close()

	emit(r, ext.PromptStateChangeEvent{State: "open"})
	emit(r, ext.TurnStateChangeEvent{State: "working"})
	emit(r, ext.PromptStateChangeEvent{State: "open"})

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if len(restarted.snapshot()) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no report delivered after the server was restarted; got %v", restarted.methods())
}

func TestHerdr_SurvivesProtocolError(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	srv.setReplyErr("stale_seq")
	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	srv.setReplyErr("")
	emit(r, ext.PromptStateChangeEvent{State: "open"})
	srv.waitFor(t, 2)

	if n := srv.connCount(); n != 1 {
		t.Fatalf("a protocol error caused a reconnect: %d connections, want 1", n)
	}
}

func TestHerdr_UnreachableServerIsSilent(t *testing.T) {
	dead := filepath.Join(t.TempDir(), "absent.sock")
	t.Setenv("HERDR_ENV", "1")
	t.Setenv("HERDR_SOCKET_PATH", dead)
	t.Setenv("HERDR_PANE_ID", "w1:p1")

	r := loadHerdr(t)

	start := time.Now()
	emit(r, ext.TurnStateChangeEvent{State: "working"})
	emit(r, ext.PromptStateChangeEvent{State: "open"})
	emit(r, ext.SessionShutdownEvent{})

	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Fatalf("handlers blocked for %v against a dead server", elapsed)
	}
}

func (s *server) lastIdleLabel() string {
	calls := s.snapshot()
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i].Method == "pane.report_metadata" {
			return calls[i].idleLabel()
		}
	}
	return ""
}

func (s *server) stateReports() []herdrCall {
	var out []herdrCall
	for _, c := range s.snapshot() {
		if c.Method == "pane.report_agent" {
			out = append(out, c)
		}
	}
	return out
}

func TestHerdr_MapsEachOutcomeToItsOwnLabel(t *testing.T) {
	cases := []struct {
		name      string
		emit      func(r *ext.Runner)
		wantLabel string
	}{
		{
			name: "completed turn is ready",
			emit: func(r *ext.Runner) {
				emit(r, ext.TurnStateChangeEvent{State: "working"})
				emit(r, ext.MessageEndEvent{Content: "All done."})
				emit(r, ext.AgentEndEvent{StopReason: "completed"})
			},
			wantLabel: "ready",
		},
		{
			name: "cancelled turn is interrupted",
			emit: func(r *ext.Runner) {
				emit(r, ext.TurnStateChangeEvent{State: "working"})
				emit(r, ext.AgentEndEvent{StopReason: "cancelled"})
			},
			wantLabel: "interrupted",
		},
		{
			name: "context-cancelled error is interrupted",
			emit: func(r *ext.Runner) {
				emit(r, ext.TurnStateChangeEvent{State: "working"})
				emit(r, ext.ErrorEvent{Error: "context canceled"})
				emit(r, ext.AgentEndEvent{StopReason: "error"})
			},
			wantLabel: "interrupted",
		},
		{
			name: "genuine provider error is failed",
			emit: func(r *ext.Runner) {
				emit(r, ext.TurnStateChangeEvent{State: "working"})
				emit(r, ext.ErrorEvent{Error: "connection reset by peer"})
				emit(r, ext.AgentEndEvent{StopReason: "error"})
			},
			wantLabel: "failed",
		},
		{
			name: "retry storm is retrying",
			emit: func(r *ext.Runner) {
				emit(r, ext.TurnStateChangeEvent{State: "working"})
				emit(r, ext.RetryEvent{Attempt: 3, Error: "429"})
			},
			wantLabel: "retrying",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newServer(t, t.TempDir())
			herdrEnv(t, srv, "w1:p1")
			r, mc := loadHerdrMock(t)

			tc.emit(r)
			waitSettled()

			if got := srv.lastIdleLabel(); got != tc.wantLabel {
				t.Errorf("idle label = %q, want %q (methods: %v)", got, tc.wantLabel, srv.methods())
			}
			if len(mc.PrintErrors) != 0 {
				t.Errorf("unexpected errors: %v", mc.PrintErrors)
			}
		})
	}
}

func TestHerdr_OutcomesStillUseValidStates(t *testing.T) {
	valid := map[string]bool{"idle": true, "working": true, "blocked": true}

	cases := map[string]func(r *ext.Runner){
		"cancelled": func(r *ext.Runner) {
			emit(r, ext.TurnStateChangeEvent{State: "working"})
			emit(r, ext.AgentEndEvent{StopReason: "cancelled"})
		},
		"error": func(r *ext.Runner) {
			emit(r, ext.TurnStateChangeEvent{State: "working"})
			emit(r, ext.ErrorEvent{Error: "500 internal"})
			emit(r, ext.AgentEndEvent{StopReason: "error"})
		},
		"completed": func(r *ext.Runner) {
			emit(r, ext.TurnStateChangeEvent{State: "working"})
			emit(r, ext.AgentEndEvent{StopReason: "completed"})
		},
	}

	for name, drive := range cases {
		t.Run(name, func(t *testing.T) {
			srv := newServer(t, t.TempDir())
			herdrEnv(t, srv, "w1:p1")
			r, mc := loadHerdrMock(t)

			drive(r)
			waitSettled()

			for _, c := range srv.stateReports() {
				if !valid[c.state()] {
					t.Errorf("reported invalid state %q; Herdr would reject the "+
						"whole report (state=%q, methods=%v)", c.state(), c.state(), srv.methods())
				}
			}
			if len(mc.PrintErrors) != 0 {
				t.Errorf("unexpected errors: %v", mc.PrintErrors)
			}
		})
	}
}

func TestHerdr_OutcomeIsRepublishedEveryTurn(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	for i := 0; i < 2; i++ {
		emit(r, ext.TurnStateChangeEvent{State: "working"})
		emit(r, ext.AgentEndEvent{StopReason: "cancelled"})
		waitSettled()
	}

	labels := 0
	for _, c := range srv.snapshot() {
		if c.Method == "pane.report_metadata" && c.idleLabel() == "interrupted" {
			labels++
		}
	}
	if labels != 2 {
		t.Fatalf("published %d 'interrupted' labels for 2 interrupted turns, want 2: %v",
			labels, srv.methods())
	}
}

func TestHerdr_OutcomeNotResentWhenUnchanged(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	emit(r, ext.AgentEndEvent{StopReason: "completed"})
	waitSettled()
	afterFirst := 0
	for _, c := range srv.snapshot() {
		if c.Method == "pane.report_metadata" {
			afterFirst++
		}
	}

	for i := 0; i < 5; i++ {
		emit(r, ext.ToolExecutionStartEvent{ToolName: "bash"})
		emit(r, ext.ToolExecutionEndEvent{ToolName: "bash"})
	}
	time.Sleep(300 * time.Millisecond)

	total := 0
	for _, c := range srv.snapshot() {
		if c.Method == "pane.report_metadata" {
			total++
		}
	}
	if total != afterFirst {
		t.Errorf("metadata re-sent with an unchanged outcome: %d -> %d", afterFirst, total)
	}
}

func TestHerdr_OutcomeLabelIsSourceGuarded(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.TurnStateChangeEvent{State: "working"})
	emit(r, ext.AgentEndEvent{StopReason: "cancelled"})
	waitSettled()

	found := false
	for _, c := range srv.snapshot() {
		if c.Method != "pane.report_metadata" {
			continue
		}
		found = true
		if got := c.param("applies_to_source"); got != "custom:kit" {
			t.Errorf("applies_to_source = %q, want custom:kit", got)
		}
		if c.has("seq") {
			t.Errorf("metadata report carried a seq: %+v", c)
		}
	}
	if !found {
		t.Fatal("no pane.report_metadata request was sent")
	}
}

func TestHerdr_ReleaseCarriesNoSeq(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	emit(r, ext.SessionShutdownEvent{})
	srv.waitFor(t, 1)

	for _, c := range srv.snapshot() {
		if c.Method == "pane.release_agent" && c.has("seq") {
			t.Errorf("release_agent carried a seq: %+v", c)
		}
	}
}

func TestHerdr_SurfacesTransportFailure(t *testing.T) {
	dead := filepath.Join(t.TempDir(), "absent.sock")
	t.Setenv("HERDR_ENV", "1")
	t.Setenv("HERDR_SOCKET_PATH", dead)
	t.Setenv("HERDR_PANE_ID", "w1:p1")

	r, mc := loadHerdrMock(t)
	emit(r, ext.TurnStateChangeEvent{State: "working"})

	if len(mc.PrintErrors) == 0 {
		t.Fatal("no diagnostic printed for an undeliverable report")
	}
	if got := mc.PrintErrors[0]; !strings.Contains(got, "pane.report_agent") {
		t.Errorf("diagnostic = %q, want it to name the failing method", got)
	}
}

func TestHerdr_ReportsFailureOnlyOnce(t *testing.T) {
	dead := filepath.Join(t.TempDir(), "absent.sock")
	t.Setenv("HERDR_ENV", "1")
	t.Setenv("HERDR_SOCKET_PATH", dead)
	t.Setenv("HERDR_PANE_ID", "w1:p1")

	r, mc := loadHerdrMock(t)

	for i := 0; i < 5; i++ {
		emit(r, ext.TurnStateChangeEvent{State: "working"})
		emit(r, ext.PromptStateChangeEvent{State: "open"})
		emit(r, ext.PromptStateChangeEvent{State: "closed"})
		emit(r, ext.TurnStateChangeEvent{State: "idle"})
	}

	if got := len(mc.PrintErrors); got != 1 {
		t.Fatalf("printed %d diagnostics for one repeated failure, want 1:\n%v",
			got, mc.PrintErrors)
	}
}

func TestHerdr_ReportsDistinctFailuresSeparately(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r, mc := loadHerdrMock(t)

	srv.setReplyErr("stale_seq")
	emit(r, ext.TurnStateChangeEvent{State: "working"})
	srv.waitFor(t, 1)

	srv.Close()
	srv.killAll()
	emit(r, ext.PromptStateChangeEvent{State: "open"})

	if len(mc.PrintErrors) < 2 {
		t.Fatalf("want a diagnostic per distinct failure, got %d:\n%v",
			len(mc.PrintErrors), mc.PrintErrors)
	}
	if !strings.Contains(mc.PrintErrors[0], "stale_seq") {
		t.Errorf("first diagnostic = %q, want it to name the rejection code", mc.PrintErrors[0])
	}
}

func TestHerdr_NoDiagnosticWhenHealthy(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r, mc := loadHerdrMock(t)

	emit(r, ext.SessionStartEvent{})
	emit(r, ext.TurnStateChangeEvent{State: "working"})
	emit(r, ext.PromptStateChangeEvent{State: "open"})
	emit(r, ext.SessionShutdownEvent{})
	srv.waitFor(t, 2)

	if len(mc.PrintErrors) != 0 {
		t.Errorf("printed %d errors on a healthy session: %v", len(mc.PrintErrors), mc.PrintErrors)
	}
	if len(mc.PrintInfos) != 0 {
		t.Errorf("printed %d info lines on a healthy session: %v", len(mc.PrintInfos), mc.PrintInfos)
	}
}

func TestHerdr_ReportsAgainAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	srv := newServer(t, dir)
	herdrEnv(t, srv, "w1:p1")
	r, mc := loadHerdrMock(t)

	srv.Close()
	srv.killAll()
	emit(r, ext.TurnStateChangeEvent{State: "working"})
	if len(mc.PrintErrors) == 0 {
		t.Fatal("expected a diagnostic for the initial failure")
	}

	restarted := listenOn(t, dir, srv.path)
	defer restarted.Close()
	emit(r, ext.PromptStateChangeEvent{State: "open"})
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && len(restarted.snapshot()) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if len(restarted.snapshot()) == 0 {
		t.Fatal("never recovered")
	}

	restarted.Close()
	restarted.killAll()
	emit(r, ext.TurnStateChangeEvent{State: "working"})

	if len(mc.PrintErrors) < 2 {
		t.Fatalf("failure after a recovery stayed suppressed; got %d diagnostics:\n%v",
			len(mc.PrintErrors), mc.PrintErrors)
	}
}

func TestHerdr_ConcurrentReportsStayOrdered(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				emit(r, ext.ToolExecutionStartEvent{ToolName: "bash"})
				emit(r, ext.TurnStateChangeEvent{State: "working"})
			}
		}()
	}
	wg.Wait()

	time.Sleep(500 * time.Millisecond)
	seqs := srv.seqs()
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("seq went backwards at %d: %v", i, seqs)
		}
	}
}

func TestHerdr_ConcurrentEmissionIsRaceFree(t *testing.T) {
	srv := newServer(t, t.TempDir())
	herdrEnv(t, srv, "w1:p1")
	r := loadHerdr(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			emit(r, ext.SessionStartEvent{})
			emit(r, ext.AgentStartEvent{})
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			emit(r, ext.MessageEndEvent{Content: "Which one?"})
			emit(r, ext.PromptStateChangeEvent{State: "open"})
		}
	}()
	wg.Wait()
	waitSettled()
}
