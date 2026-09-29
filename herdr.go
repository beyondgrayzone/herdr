//go:build ignore

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"kit/ext"
)

const (
	herdrEnvVar   = "HERDR_ENV"
	herdrSockVar  = "HERDR_SOCKET_PATH"
	herdrPaneVar  = "HERDR_PANE_ID"
	herdrSource   = "custom:kit"
	herdrAgent    = "kit"
	herdrSockDial = 500 * time.Millisecond
	herdrCallTO   = 2 * time.Second
)

type herdrEndpoint struct {
	socket string
	paneID string
}

func resolveHerdrEndpoint() *herdrEndpoint {
	if os.Getenv(herdrEnvVar) != "1" {
		return nil
	}
	sock := strings.TrimSpace(os.Getenv(herdrSockVar))
	pane := strings.TrimSpace(os.Getenv(herdrPaneVar))
	if sock == "" || pane == "" {
		return nil
	}
	return &herdrEndpoint{socket: sock, paneID: pane}
}

type herdrReporter struct {
	ep *herdrEndpoint

	mu     sync.Mutex
	seq    int64
	last   string // last state actually reported, for de-duplication
	closed bool

	conn net.Conn

	reuseConn bool

	reported string

	lastOutcome string
}

var reporter *herdrReporter

func initReporter() *herdrReporter {
	ep := resolveHerdrEndpoint()
	if ep == nil {
		reporter = nil
		return nil
	}
	reporter = &herdrReporter{ep: ep, reuseConn: true}
	return reporter
}

func (r *herdrReporter) markClosed() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
}

func (r *herdrReporter) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func socketNetwork() string {
	if runtime.GOOS == "windows" {
		return "npipe"
	}
	return "unix"
}

func dialSocket(path string) (net.Conn, error) {
	network := socketNetwork()
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		conn, err := net.DialTimeout(network, path, herdrSockDial)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}
	return nil, lastErr
}

type herdrResponse struct {
	ID     string `json:"id"`
	Result struct {
		Type string `json:"type"`
	} `json:"result"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type herdrRequest struct {
	ID     string                 `json:"id"`
	Method string                 `json:"method"`
	Params map[string]interface{} `json:"params"`
}

func (r *herdrReporter) herdrCall(method string, params map[string]interface{}, report bool) error {
	err := r.exchange(method, params, r.mintSeqLocked(params, report))
	if err == nil {
		return nil
	}
	if !isStaleConn(err) {
		return err
	}

	r.dropConn()
	r.reuseConn = false
	return r.exchange(method, params, r.mintSeqLocked(params, report))
}

func (r *herdrReporter) mintSeqLocked(params map[string]interface{}, report bool) int64 {
	seq := r.nextSeqLocked()
	if report {
		params["seq"] = seq
	}
	return seq
}

func (r *herdrReporter) exchange(method string, params map[string]interface{}, seq int64) error {
	if r.conn == nil || !r.reuseConn {
		r.dropConn()
		conn, err := dialSocket(r.ep.socket)
		if err != nil {
			return err
		}
		r.conn = conn
	}

	req := herdrRequest{
		ID:     herdrRequestID(seq),
		Method: method,
		Params: params,
	}
	line, err := json.Marshal(req)
	if err != nil {
		r.dropConn()
		return err
	}
	line = append(line, '\n')

	if err := r.conn.SetDeadline(time.Now().Add(herdrCallTO)); err != nil {
		r.dropConn()
		return err
	}
	if _, err := r.conn.Write(line); err != nil {
		r.dropConn()
		return err
	}

	var resp herdrResponse
	if err := json.NewDecoder(r.conn).Decode(&resp); err != nil {
		r.dropConn()
		return err
	}
	if resp.Error != nil {
		return herdrProtocolError{code: resp.Error.Code, message: resp.Error.Message}
	}
	return nil
}

func isStaleConn(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	msg := err.Error()
	for _, marker := range []string{
		"broken pipe",
		"connection reset",
		"connection refused",
		"EOF",
		"closed network connection",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func herdrRequestID(seq int64) string {
	return "kit-" + strconv.FormatInt(seq, 10)
}

func (r *herdrReporter) nextSeqLocked() int64 {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	if now <= r.seq {
		now = r.seq + 1
	}
	r.seq = now
	return r.seq
}

func (r *herdrReporter) dropConn() {
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}
}

func (r *herdrReporter) closeConn() {
	r.dropConn()
}

type herdrProtocolError struct {
	code    string
	message string
}

func (e herdrProtocolError) Error() string {
	if e.message == "" {
		return e.code
	}
	return e.code + ": " + e.message
}

func isProtocolError(err error) (herdrProtocolError, bool) {
	pe, ok := err.(herdrProtocolError)
	return pe, ok
}

func (r *herdrReporter) herdrDiagnose(ctx ext.Context, op string, err error) {
	if err == nil {
		return
	}
	key := op + "|" + err.Error()
	if r.reported == key {
		return
	}
	r.reported = key

	msg := fmt.Sprintf("herdr: %s failed: %v", op, err)
	if pe, ok := isProtocolError(err); ok {
		msg = fmt.Sprintf("herdr: %s rejected by Herdr (%s)", op, pe.code)
	}
	if ctx.PrintError != nil {
		ctx.PrintError(msg)
	}
}

func (r *herdrReporter) clearDiagnose() {
	r.reported = ""
}

func clearOutcomeLabel() {
	r := reporter
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clearOutcome()
}

func reportOutcome(ctx ext.Context, o herdrOutcome) {
	r := reporter
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	label := o.label()
	if r.lastOutcome == label {
		return
	}
	r.lastOutcome = label

	params := map[string]interface{}{
		"pane_id":           r.ep.paneID,
		"source":            herdrSource,
		"agent":             herdrAgent,
		"applies_to_source": herdrSource,
		"state_labels": map[string]interface{}{
			herdrIdle: label,
		},
	}
	if err := r.herdrCall("pane.report_metadata", params, false); err != nil {
		r.herdrDiagnose(ctx, "pane.report_metadata", err)
		r.lastOutcome = ""
	}
}

func (r *herdrReporter) clearOutcome() {
	r.lastOutcome = ""
}

func (r *herdrReporter) report(ctx ext.Context, state, message string) {
	if r == nil || r.isClosed() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	params := map[string]interface{}{
		"pane_id": r.ep.paneID,
		"source":  herdrSource,
		"agent":   herdrAgent,
		"state":   state,
	}
	if message != "" {
		params["message"] = message
	}
	if err := r.herdrCall("pane.report_agent", params, true); err != nil {
		r.herdrDiagnose(ctx, "pane.report_agent", err)
		return
	}
	r.clearDiagnose()
}

func (r *herdrReporter) release(ctx ext.Context) {
	if r == nil {
		return
	}
	r.markClosed()

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.herdrCall("pane.release_agent", map[string]interface{}{
		"pane_id": r.ep.paneID,
		"source":  herdrSource,
		"agent":   herdrAgent,
	}, false); err != nil {
		if ctx.PrintInfo != nil {
			ctx.PrintInfo(fmt.Sprintf("herdr: pane.release_agent failed: %v", err))
		}
	}
	r.closeConn()
}

var lastErrorWasCancel bool

var cancelErrorMarkers = []string{
	"context canceled",
	"context cancelled",
	"operation was canceled",
	"operation was cancelled",
	"request canceled",
	"request cancelled",
	"err_canceled",
	"err_cancelled",
}

func looksCancelled(msg string) bool {
	m := strings.ToLower(msg)
	for _, marker := range cancelErrorMarkers {
		if strings.Contains(m, marker) {
			return true
		}
	}
	return false
}

func noteHerdrErrorText(text string) {
	r := reporter
	if r == nil {
		lastErrorWasCancel = looksCancelled(text)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	lastErrorWasCancel = looksCancelled(text)
}

const (
	herdrWorking = "working"
	herdrIdle    = "idle"
	herdrBlocked = "blocked"
)

const (
	herdrLabelReady       = "ready"
	herdrLabelInterrupted = "interrupted"
	herdrLabelFailed      = "failed"
	herdrLabelRetrying    = "retrying"
)

type herdrOutcome int

const (
	herdrOutcomeReady herdrOutcome = iota
	herdrOutcomeInterrupted
	herdrOutcomeFailed
	herdrOutcomeRetrying
)

func (o herdrOutcome) state() string {
	switch o {
	case herdrOutcomeRetrying:
		return herdrWorking
	default:
		return herdrIdle
	}
}

func (o herdrOutcome) label() string {
	switch o {
	case herdrOutcomeInterrupted:
		return herdrLabelInterrupted
	case herdrOutcomeFailed:
		return herdrLabelFailed
	case herdrOutcomeRetrying:
		return herdrLabelRetrying
	default:
		return herdrLabelReady
	}
}

func reportState(ctx ext.Context, state, message string) {
	r := reporter
	if r == nil || state == "" {
		return
	}
	r.mu.Lock()
	if r.last == state && message == "" {
		r.mu.Unlock()
		return
	}
	r.last = state
	r.mu.Unlock()

	r.report(ctx, state, message)
}

func reportProgress(ctx ext.Context, state, message string) {
	if !herdrStateUnclaimed() || herdrModalIsOpen() {
		return
	}
	reportState(ctx, state, message)
}

var herdrTurn struct {
	mu           sync.Mutex
	turnGen      int
	turnEnded    bool
	turnIsCancel bool
	outcome      herdrOutcome
	turnSettling bool
	turnAsksUser bool
	modalOpen    bool
}

func resetHerdrTurn() {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	herdrTurn.turnEnded = false
	herdrTurn.turnIsCancel = false
	herdrTurn.turnSettling = false
	herdrTurn.turnAsksUser = false
}

func beginHerdrTurn() {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	herdrTurn.turnGen++
	herdrTurn.turnEnded = false
	herdrTurn.turnIsCancel = false
	herdrTurn.outcome = herdrOutcomeReady
	herdrTurn.turnSettling = false
	herdrTurn.turnAsksUser = false
}

func currentHerdrGen() int {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	return herdrTurn.turnGen
}

func genIsCurrentHerdr(gen int) bool {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	return herdrTurn.turnGen == gen
}

func markHerdrSettlePending() {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	herdrTurn.turnSettling = true
}

func markHerdrStillRunning() {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	herdrTurn.turnSettling = false
	herdrTurn.turnEnded = false
}

func claimHerdrVerdict(o herdrOutcome) bool {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	if herdrTurn.turnEnded {
		return false
	}
	herdrTurn.turnEnded = true
	herdrTurn.turnIsCancel = o == herdrOutcomeInterrupted
	herdrTurn.outcome = o
	return true
}

func herdrStateUnclaimed() bool {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	return !herdrTurn.turnEnded && !herdrTurn.turnSettling
}

func herdrTurnWasCancelled() bool {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	return herdrTurn.turnIsCancel
}

func herdrTurnAskedUser() bool {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	return herdrTurn.turnAsksUser
}

func noteHerdrResponse(content string) {
	asks := endsWithQuestion(content)
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	herdrTurn.turnAsksUser = asks
}

func herdrModalIsOpen() bool {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	return herdrTurn.modalOpen
}

func setHerdrModalOpen(open bool) {
	herdrTurn.mu.Lock()
	defer herdrTurn.mu.Unlock()
	herdrTurn.modalOpen = open
}

func endsWithQuestion(content string) bool {
	tail := trailingText(content)
	if !hasQuestionTrigger(tail) {
		return false
	}
	return finalSentenceAsks(tail)
}

func trailingText(content string) string {
	closing := strings.TrimRight(stripFencedCode(content), "*_` \t\r\n")
	tail := closing
	if _, after, found := strings.Cut(closing, "\n\n"); found {
		tail = after
	} else if _, after, found := strings.Cut(closing, "\n"); found {
		tail = after
	}
	return strings.TrimRight(tail, "*_` \t\r\n")
}

func hasQuestionTrigger(tail string) bool {
	for i := len(tail); i > 0; {
		r, size := utf8DecodeLastRuneInString(tail[:i])
		if !isClosingPunct(r) {
			return false
		}
		if r == '?' || r == '？' {
			return true
		}
		i -= size
	}
	return false
}

func finalSentenceAsks(tail string) bool {
	words := lastSentence(tail)
	if words == "" {
		return false
	}
	if strings.HasSuffix(words, "#") {
		return false
	}
	if idx := listMarkerOffset(words); idx >= 0 {
		words = strings.TrimSpace(words[idx:])
	}
	if strings.ContainsAny(words, "!?！") {
		return !strings.ContainsAny(words, ";；")
	}
	if hasLeadingQuestionWord(words) {
		return true
	}
	return !strings.ContainsAny(words, ",;:.!?\n") &&
		!strings.ContainsAny(words, "，；：、\r")
}

func lastSentence(tail string) string {
	i := len(tail)
	for i > 0 {
		r, size := utf8DecodeLastRuneInString(tail[:i])
		if !isClosingPunct(r) {
			break
		}
		i -= size
	}

	start := 0
	for j := 0; j < i; {
		r, size := utf8DecodeRuneInString(tail[j:i])
		if isSentenceMark(r) {
			start = j + size
		}
		j += size
	}
	return strings.TrimSpace(tail[start:i])
}

func isSentenceMark(r rune) bool {
	switch r {
	case '.', '!', '?', '…', '。', '．', '！', '？', '\n':
		return true
	}
	return false
}

func listMarkerOffset(s string) int {
	best := -1
	offset := 0
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimLeft(line, " \t")
		if marker := markerWidth(trimmed); marker > 0 {
			best = offset + len(line) - len(trimmed) + marker
		}
		offset += len(line) + 1
	}
	return best
}

func markerWidth(s string) int {
	for _, p := range []string{"- ", "* ", "+ ", "• "} {
		if strings.HasPrefix(s, p) {
			return len(p)
		}
	}
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i > 0 && i+1 < len(s) && (s[i] == '.' || s[i] == ')') && s[i+1] == ' ' {
		return i + 2
	}
	return 0
}

func hasLeadingQuestionWord(sentence string) bool {
	trimmed := strings.TrimLeft(sentence, "*_` \t>#")
	trimmed = strings.TrimLeft(trimmed, "-–•")
	trimmed = strings.TrimLeft(trimmed, " \t")

	for _, w := range questionOpeners {
		if len(trimmed) < len(w) {
			continue
		}
		if !strings.EqualFold(trimmed[:len(w)], w) {
			continue
		}
		rest := trimmed[len(w):]
		if rest == "" {
			return true
		}
		r, _ := utf8DecodeRuneInString(rest)
		if !isLetterOrDigit(r) {
			return true
		}
	}
	return false
}

var questionOpeners = []string{
	"what", "which", "who", "whom", "whose", "where", "when", "why", "how",
}

func isClosingPunct(r rune) bool {
	switch r {
	case '.', ',', ':', ';', '!', '?',
		'。', '．', '，', '：', '；', '！', '？', '、', '…',
		'\n', '\r', ')', ']', '}', '"', '\'', '”', '’', '`':
		return true
	}
	return false
}

func stripFencedCode(content string) string {
	var b strings.Builder
	rest := content
	for {
		idx, marker := nextFence(rest)
		if idx < 0 {
			b.WriteString(rest)
			return b.String()
		}
		b.WriteString(rest[:idx])
		next := strings.Index(rest[idx+len(marker):], marker)
		if next < 0 {
			b.WriteString(rest[idx:])
			return b.String()
		}
		rest = rest[idx+len(marker)+next+len(marker):]
	}
}

func nextFence(s string) (int, string) {
	backtick := strings.Index(s, "```")
	tilde := strings.Index(s, "~~~")
	switch {
	case backtick < 0 && tilde < 0:
		return -1, ""
	case tilde < 0 || (backtick >= 0 && backtick < tilde):
		return backtick, "```"
	default:
		return tilde, "~~~"
	}
}

func utf8DecodeRuneInString(s string) (rune, int) {
	r, size := utf8.DecodeRuneInString(s)
	return r, size
}

func utf8DecodeLastRuneInString(s string) (rune, int) {
	r, size := utf8.DecodeLastRuneInString(s)
	return r, size
}

func isLetterOrDigit(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

const settleDelay = 400 * time.Millisecond

func settleAfter(ctx ext.Context, outcome herdrOutcome) {
	markHerdrSettlePending()
	gen := currentHerdrGen()
	reported := outcome.state()

	go func() {
		timer := time.NewTimer(settleDelay)
		defer timer.Stop()
		<-timer.C

		if !genIsCurrentHerdr(gen) {
			return
		}

		if herdrModalIsOpen() {
			markHerdrStillRunning()
			reportState(ctx, herdrBlocked, "modal open")
			return
		}

		if ctx.IsIdle == nil {
			reportState(ctx, reported, "")
			return
		}
		if !ctx.IsIdle() {
			markHerdrStillRunning()
			if herdrStateUnclaimed() {
				reportState(ctx, herdrBlocked, "waiting on user")
			}
			return
		}

		if reported == herdrIdle && herdrTurnWasCancelled() {
			reportState(ctx, herdrIdle, "")
			reportOutcome(ctx, herdrOutcomeInterrupted)
			return
		}
		if reported == herdrIdle && herdrTurnAskedUser() {
			reportState(ctx, herdrBlocked, "")
			reportOutcome(ctx, herdrOutcomeReady)
			return
		}
		reportState(ctx, reported, "")
		reportOutcome(ctx, outcome)
	}()
}

func Init(api ext.API) {
	if initReporter() == nil {
		return
	}

	api.OnSessionStart(func(_ ext.SessionStartEvent, ctx ext.Context) {
		resetHerdrTurn()
		ctx.PrintInfo("HERDR loaded")
		reportState(ctx, herdrIdle, "")
	})

	api.OnSessionShutdown(func(_ ext.SessionShutdownEvent, ctx ext.Context) {
		if reporter != nil {
			reporter.release(ctx)
		}
	})

	api.OnTurnStateChange(func(e ext.TurnStateChangeEvent, ctx ext.Context) {
		if e.State == "working" {
			beginHerdrTurn()
			clearOutcomeLabel()
			reportState(ctx, herdrWorking, "")
			return
		}
		if herdrModalIsOpen() {
			reportState(ctx, herdrBlocked, "modal open")
			return
		}
		if herdrStateUnclaimed() {
			reportState(ctx, herdrIdle, "")
		}
	})

	api.OnPromptStateChange(func(e ext.PromptStateChangeEvent, ctx ext.Context) {
		if e.State == "open" {
			setHerdrModalOpen(true)
			reportState(ctx, herdrBlocked, "modal open")
			return
		}
		setHerdrModalOpen(false)
	})

	api.OnAgentStart(func(_ ext.AgentStartEvent, ctx ext.Context) {
		beginHerdrTurn()
		clearOutcomeLabel()
		reportState(ctx, herdrWorking, "")
	})

	api.OnAgentEnd(func(e ext.AgentEndEvent, ctx ext.Context) {
		var outcome herdrOutcome
		switch e.StopReason {
		case "error":
			outcome = herdrOutcomeFailed
			if lastErrorWasCancel {
				outcome = herdrOutcomeInterrupted
			}
		case "cancelled":
			outcome = herdrOutcomeInterrupted
		default:
			outcome = herdrOutcomeReady
		}
		if claimHerdrVerdict(outcome) {
			settleAfter(ctx, outcome)
		}
	})

	api.OnToolExecutionStart(func(e ext.ToolExecutionStartEvent, ctx ext.Context) {
		if e.ToolName == "" {
			return
		}
		reportProgress(ctx, herdrWorking, "")
	})

	api.OnToolExecutionEnd(func(e ext.ToolExecutionEndEvent, ctx ext.Context) {
		if e.ToolName == "" {
			return
		}
		reportProgress(ctx, herdrWorking, "")
	})

	api.OnError(func(e ext.ErrorEvent, ctx ext.Context) {
		text := strings.TrimSpace(e.Error)
		if text == "" {
			return
		}
		noteHerdrErrorText(text)
		reportProgress(ctx, herdrWorking, "")
		if looksCancelled(text) {
			return
		}
		reportOutcome(ctx, herdrOutcomeRetrying)
	})

	api.OnRetry(func(e ext.RetryEvent, ctx ext.Context) {
		reportProgress(ctx, herdrWorking, "")
		if e.Attempt > 1 {
			reportOutcome(ctx, herdrOutcomeRetrying)
		}
	})

	api.OnSubagentStart(func(_ ext.SubagentStartEvent, ctx ext.Context) {
		reportProgress(ctx, herdrWorking, "")
	})

	api.OnSubagentEnd(func(_ ext.SubagentEndEvent, ctx ext.Context) {
		reportProgress(ctx, herdrWorking, "")
	})

	api.OnMessageEnd(func(e ext.MessageEndEvent, _ ext.Context) {
		noteHerdrResponse(e.Content)
	})
}
