package nntp

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	nntpyenc "github.com/sirrobot01/decypharr/internal/nntp/yenc"
)

// Note: Timeout values are defined in TimeoutConfig (client.go)
// Use timeouts.timeouts.StreamBodyTimeout for read deadlines

// Connection represents an NNTP connection
type Connection struct {
	username, password, address string
	port                        int
	conn                        net.Conn
	text                        *textproto.Reader
	reader                      *bufio.Reader
	writer                      *bufio.Writer
	logger                      zerolog.Logger
	closed                      atomic.Bool
}

func (c *Connection) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.conn.Close()
}

func (c *Connection) IsClosed() bool {
	return c.closed.Load()
}

func (c *Connection) authenticate() error {
	// Send AUTHINFO USER command
	if err := c.sendCommand(fmt.Sprintf("AUTHINFO USER %s", c.username)); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send username: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read user response: %w", err))
	}

	if resp.Code != 381 {
		return classifyNNTPError(resp.Code, fmt.Sprintf("unexpected response to AUTHINFO USER: %s", resp.Message))
	}

	// Send AUTHINFO PASS command
	if err := c.sendCommand(fmt.Sprintf("AUTHINFO PASS %s", c.password)); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send password: %w", err))
	}

	resp, err = c.readResponse()
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read password response: %w", err))
	}

	if resp.Code != 281 {
		return classifyNNTPError(resp.Code, fmt.Sprintf("[%s] authentication failed: %s", c.address, resp.Message))
	}
	return nil
}

// startTLS initiates TLS encryption with proper error handling
func (c *Connection) startTLS() error {
	if err := c.sendCommand("STARTTLS"); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send STARTTLS: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read STARTTLS response: %w", err))
	}

	if resp.Code != 382 {
		return classifyNNTPError(resp.Code, fmt.Sprintf("STARTTLS not supported: %s", resp.Message))
	}

	// Upgrade connection to TLS
	tlsConn := tls.Client(c.conn, &tls.Config{
		ServerName:         c.address,
		InsecureSkipVerify: true, // Match createConnection behavior
		MinVersion:         tls.VersionTLS12,
	})

	c.conn = tlsConn
	c.reader = bufio.NewReaderSize(tlsConn, 256*1024)
	c.writer = bufio.NewWriterSize(tlsConn, 256*1024)
	c.text = textproto.NewReader(c.reader)

	c.logger.Debug().Msg("TLS encryption enabled")
	return nil
}

// ping sends a simple command to test the connection
func (c *Connection) ping() error {
	if err := c.sendCommand("DATE"); err != nil {
		return NewConnectionError(err)
	}
	_, err := c.readResponse()
	if err != nil {
		return NewConnectionError(err)
	}
	return nil
}

// sendCommand sends a command to the NNTP server
func (c *Connection) sendCommand(command string) error {
	_, err := fmt.Fprintf(c.writer, "%s\r\n", command)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

// readResponse reads a response from the NNTP server
func (c *Connection) readResponse() (*Response, error) {
	line, err := c.text.ReadLine()
	if err != nil {
		return nil, err
	}

	parts := strings.SplitN(line, " ", 2)
	code, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid response code: %s", parts[0])
	}

	message := ""
	if len(parts) > 1 {
		message = parts[1]
	}

	return &Response{
		Code:    code,
		Message: message,
	}, nil
}

// readMultilineResponse reads a multiline response
func (c *Connection) readMultilineResponse() (*Response, error) {
	resp, err := c.readResponse()
	if err != nil {
		return nil, err
	}

	// Check if this is a multiline response
	if resp.Code < 200 || resp.Code >= 300 {
		return resp, nil
	}

	lines, err := c.text.ReadDotLines()
	if err != nil {
		return nil, err
	}

	resp.Lines = lines
	return resp, nil
}

// GetArticle retrieves an article by message ID with proper error classification
func (c *Connection) GetArticle(messageID string) (*Article, error) {
	messageID = FormatMessageID(messageID)
	if err := c.sendCommand(fmt.Sprintf("ARTICLE %s", messageID)); err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to send ARTICLE command: %w", err))
	}

	resp, err := c.readMultilineResponse()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read article response: %w", err))
	}

	if resp.Code != 220 {
		return nil, classifyNNTPError(resp.Code, resp.Message)
	}

	return c.parseArticle(messageID, resp.Lines)
}

func (c *Connection) GetHeader(messageID string, maxSnippet int) (*YencMetadata, error) {
	messageID = FormatMessageID(messageID)
	// Send BODY command to start streaming
	if err := c.sendCommand(fmt.Sprintf("BODY %s", messageID)); err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to send BODY command: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read body response: %w", err))
	}

	if resp.Code != 222 {
		return nil, classifyNNTPError(resp.Code, resp.Message)
	}

	// Set read deadline to prevent hanging on stalled servers
	_ = c.conn.SetReadDeadline(time.Now().Add(timeouts.StreamBodyTimeout))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()

	dec := nntpyenc.AcquireDecoder(c.reader)
	defer nntpyenc.ReleaseDecoder(dec)

	// Read snippet to trigger header parsing and capture metadata.
	snippet := make([]byte, maxSnippet)
	n, err := io.ReadFull(dec, snippet)
	if err != nil && err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
		_ = c.conn.Close()
		return nil, fmt.Errorf("failed to read snippet: %w", err)
	}
	// Truncate snippet to actual read size
	snippet = snippet[:n]
	meta := &YencMetadata{
		Name:     dec.Meta.FileName,
		Size:     dec.Meta.FileSize,
		Part:     dec.Meta.PartNumber,
		Total:    dec.Meta.TotalParts,
		Offset:   dec.Meta.Offset,
		PartSize: dec.Meta.PartSize,
		Begin:    dec.Meta.Begin(),
		End:      dec.Meta.End(),
		Snippet:  snippet,
	}

	// Close connection to stop stream
	_ = c.Close()

	return meta, nil
}

// GetBody retrieves article body by message ID as raw bytes (used by GetHeader)
func (c *Connection) GetBody(messageID string) ([]byte, error) {
	messageID = FormatMessageID(messageID)
	if err := c.sendCommand(fmt.Sprintf("BODY %s", messageID)); err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to send BODY command: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read body response: %w", err))
	}

	if resp.Code != 222 {
		return nil, classifyNNTPError(resp.Code, resp.Message)
	}

	// Set read deadline to prevent hanging on stalled servers
	_ = c.conn.SetReadDeadline(time.Now().Add(timeouts.StreamBodyTimeout))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()

	return c.readDotBytes()
}

// GetDecodedBody retrieves and decodes article body using streaming yEnc decode.
// Uses textproto.DotReader + rapidyenc streaming decoder to decode while reading
// from the network - no intermediate buffering of the full body.
func (c *Connection) GetDecodedBody(messageID string) ([]byte, error) {
	messageID = FormatMessageID(messageID)
	if err := c.sendCommand(fmt.Sprintf("BODY %s", messageID)); err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to send BODY command: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read body response: %w", err))
	}

	if resp.Code != 222 {
		return nil, classifyNNTPError(resp.Code, resp.Message)
	}

	// Set read deadline to prevent hanging on stalled servers
	_ = c.conn.SetReadDeadline(time.Now().Add(timeouts.StreamBodyTimeout))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()

	dec := nntpyenc.AcquireDecoder(c.reader)
	// Always release decoder back to pool, even on panic
	defer nntpyenc.ReleaseDecoder(dec)

	// Pre-allocate output buffer for decoded data (~700KB typical)
	output := bytes.NewBuffer(make([]byte, 0, 750*1024))
	_, err = io.Copy(output, dec)

	if err != nil {
		return nil, fmt.Errorf("streaming yenc decode failed: %w", err)
	}
	decoded := output.Bytes()

	return decoded, nil
}

func (c *Connection) StreamBody(messageID string, w io.Writer) (int64, error) {
	messageID = FormatMessageID(messageID)
	if err := c.sendCommand(fmt.Sprintf("BODY %s", messageID)); err != nil {
		return 0, NewConnectionError(fmt.Errorf("failed to send BODY command: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return 0, NewConnectionError(fmt.Errorf("failed to read body response: %w", err))
	}

	if resp.Code != 222 {
		return 0, classifyNNTPError(resp.Code, resp.Message)
	}

	// Set read deadline to prevent hanging if server stops sending
	_ = c.conn.SetReadDeadline(time.Now().Add(timeouts.StreamBodyTimeout))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }() // Clear deadline

	dec := nntpyenc.AcquireDecoder(c.reader)
	// Always release decoder back to pool, even on panic
	defer nntpyenc.ReleaseDecoder(dec)
	n, err := io.Copy(w, dec)
	if err != nil {
		return n, fmt.Errorf("streaming yenc decode failed: %w", err)
	}
	return n, nil
}

// readDotBytes reads dot-terminated NNTP data using textproto.DotReader
// This matches Python nntplib's efficient buffered approach
func (c *Connection) readDotBytes() ([]byte, error) {
	// Use textproto's DotReader which efficiently handles dot-stuffing
	// and terminator detection with optimized buffered reading
	dotReader := c.text.DotReader()

	// Pre-allocate for typical usenet segment (~750KB)
	// Using io.ReadAll with pre-sized buffer hint
	buf := bytes.NewBuffer(make([]byte, 0, 800*1024))

	// Copy from DotReader to buffer
	_, err := io.Copy(buf, dotReader)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// GetHead retrieves article headers by message ID
func (c *Connection) GetHead(messageID string) ([]byte, error) {
	messageID = FormatMessageID(messageID)
	if err := c.sendCommand(fmt.Sprintf("HEAD %s", messageID)); err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to send HEAD command: %w", err))
	}

	// Read the initial response
	resp, err := c.readResponse()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read head response: %w", err))
	}

	if resp.Code != 221 {
		return nil, classifyNNTPError(resp.Code, resp.Message)
	}

	// Read the header data using textproto
	lines, err := c.text.ReadDotLines()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read header data: %w", err))
	}

	// Join with \r\n to preserve original line endings and add final \r\n
	headers := strings.Join(lines, "\r\n")
	if len(lines) > 0 {
		headers += "\r\n"
	}

	return []byte(headers), nil
}

func (c *Connection) Post(messageID, filename string, body []byte) error {
	now := time.Now().Format("2006-01-02 15:04:05")
	if err := c.sendCommand("POST"); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send POST command: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read POST response: %w", err))
	}

	// 340 = send article to be posted
	if resp.Code != 340 {
		// 440, 441, etc should be classified properly
		return classifyNNTPError(resp.Code, fmt.Sprintf("unexpected response to POST: %s", resp.Message))
	}

	// 2. Build RFC-822 style article (headers + blank line + body)
	var buf bytes.Buffer

	if filename != "" {
		buf.WriteString("Subject: " + filename + "\r\n")
	}

	buf.WriteString("Date: " + now + "\r\n")
	buf.WriteString("Newsgroups: " + "alt.binaries.friends" + "\r\n")
	if messageID != "" {
		// ensure proper <id> format
		msgID := FormatMessageID(messageID)
		buf.WriteString("Message-ID: " + msgID + "\r\n")
	}

	// End of headers
	buf.WriteString("\r\n")

	// 3. Body with CRLF normalization + dot-stuffing
	if len(body) > 0 {
		// Normalize to \n, then re-add \r\n
		body := bytes.ReplaceAll(body, []byte("\r\n"), []byte("\n"))
		lines := bytes.Split(body, []byte("\n"))

		for _, line := range lines {
			// Last split after trailing \n will give empty line; still write CRLF.
			if len(line) > 0 && line[0] == '.' {
				// dot-stuff per NNTP
				buf.WriteByte('.')
			}
			buf.Write(line)
			buf.WriteString("\r\n")
		}
	}

	// 4. Terminator line
	buf.WriteString(".\r\n")

	// 5. Send article data
	if _, err := c.writer.Write(buf.Bytes()); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send article data: %w", err))
	}
	if err := c.writer.Flush(); err != nil {
		return NewConnectionError(fmt.Errorf("failed to flush article data: %w", err))
	}

	// 6. Final response
	resp, err = c.readResponse()
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read post completion response: %w", err))
	}

	if resp.Code != 240 { // 240 = article received OK
		return classifyNNTPError(resp.Code, resp.Message)
	}

	return nil
}

// Stat retrieves article statistics by message ID with proper error classification
func (c *Connection) Stat(messageID string) (articleNumber int, echoedID string, err error) {
	messageID = FormatMessageID(messageID)

	if err = c.sendCommand(fmt.Sprintf("STAT %s", messageID)); err != nil {
		return 0, "", NewConnectionError(fmt.Errorf("failed to send STAT: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return 0, "", NewConnectionError(fmt.Errorf("failed to read STAT response: %w", err))
	}

	if resp.Code != 223 {
		return 0, "", classifyNNTPError(resp.Code, resp.Message)
	}

	fields := strings.Fields(resp.Message)
	if len(fields) < 2 {
		return 0, "", NewProtocolError(resp.Code, fmt.Sprintf("unexpected STAT response format: %q", resp.Message))
	}

	if articleNumber, err = strconv.Atoi(fields[0]); err != nil {
		return 0, "", NewProtocolError(resp.Code, fmt.Sprintf("invalid article number %q: %v", fields[0], err))
	}
	echoedID = fields[1]

	return articleNumber, echoedID, nil
}

// PipelinedStat sends multiple STAT commands in a pipeline and reads all responses.
// This is much more efficient than individual Stat calls as it reduces round-trip latency.
// Returns per-segment results so caller can identify exactly which segments are missing.
// On connection/protocol error, returns immediately as the connection state is invalid.
func (c *Connection) PipelinedStat(messageIDs []string) ([]StatResult, error) {
	if len(messageIDs) == 0 {
		return nil, nil
	}

	results := make([]StatResult, len(messageIDs))

	// Phase 1: Send all STAT commands without waiting for responses
	for _, msgID := range messageIDs {
		formatted := FormatMessageID(msgID)
		if err := c.sendCommand(fmt.Sprintf("STAT %s", formatted)); err != nil {
			return nil, NewConnectionError(fmt.Errorf("failed to send STAT for %s: %w", msgID, err))
		}
	}

	// Phase 2: Read all responses, collecting per-segment results
	for i, msgID := range messageIDs {
		results[i].MessageID = msgID

		resp, err := c.readResponse()
		if err != nil {
			// Network/Protocol error: Stop immediately as connection state is likely lost/invalid.
			// Mark remaining as errors
			for j := i; j < len(messageIDs); j++ {
				results[j].MessageID = messageIDs[j]
				results[j].Available = false
				results[j].Error = NewConnectionError(fmt.Errorf("connection lost at segment %d/%d", i+1, len(messageIDs)))
			}
			return results, NewConnectionError(fmt.Errorf("failed to read STAT response %d/%d for %s: %w", i+1, len(messageIDs), msgID, err))
		}

		if resp.Code == 223 {
			results[i].Available = true
		} else {
			// Article not found or other error - record but continue draining
			results[i].Available = false
			results[i].Error = classifyNNTPError(resp.Code, fmt.Sprintf("segment %s: %s", msgID, resp.Message))
		}
	}

	return results, nil
}

// SelectGroup selects a newsgroup and returns group information
func (c *Connection) SelectGroup(groupName string) (*GroupInfo, error) {
	if err := c.sendCommand(fmt.Sprintf("GROUP %s", groupName)); err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to send GROUP command: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read GROUP response: %w", err))
	}

	if resp.Code != 211 {
		return nil, classifyNNTPError(resp.Code, resp.Message)
	}

	// Parse GROUP response: "211 number low high group-name"
	fields := strings.Fields(resp.Message)
	if len(fields) < 4 {
		return nil, NewProtocolError(resp.Code, fmt.Sprintf("unexpected GROUP response format: %q", resp.Message))
	}

	groupInfo := &GroupInfo{
		Name: groupName,
	}

	if count, err := strconv.Atoi(fields[0]); err == nil {
		groupInfo.Count = count
	}
	if low, err := strconv.Atoi(fields[1]); err == nil {
		groupInfo.Low = low
	}
	if high, err := strconv.Atoi(fields[2]); err == nil {
		groupInfo.High = high
	}

	return groupInfo, nil
}

// parseArticle parses article data from response lines
func (c *Connection) parseArticle(messageID string, lines []string) (*Article, error) {
	article := &Article{
		MessageID: messageID,
		Groups:    []string{},
	}

	headerEnd := -1
	for i, line := range lines {
		if line == "" {
			headerEnd = i
			break
		}

		// Parse headers
		if strings.HasPrefix(line, "Subject: ") {
			article.Subject = strings.TrimPrefix(line, "Subject: ")
		} else if strings.HasPrefix(line, "From: ") {
			article.From = strings.TrimPrefix(line, "From: ")
		} else if strings.HasPrefix(line, "Date: ") {
			article.Date = strings.TrimPrefix(line, "Date: ")
		} else if strings.HasPrefix(line, "Newsgroups: ") {
			groups := strings.TrimPrefix(line, "Newsgroups: ")
			article.Groups = strings.Split(groups, ",")
			for i := range article.Groups {
				article.Groups[i] = strings.TrimSpace(article.Groups[i])
			}
		}
	}

	// Join body lines
	if headerEnd != -1 && headerEnd+1 < len(lines) {
		body := strings.Join(lines[headerEnd+1:], "\n")
		article.Body = []byte(body)
		article.Size = int64(len(article.Body))
	}

	return article, nil
}

// FormatMessageID ensures message ID has proper format
func FormatMessageID(messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if !strings.HasPrefix(messageID, "<") {
		messageID = "<" + messageID
	}
	if !strings.HasSuffix(messageID, ">") {
		messageID = messageID + ">"
	}
	return messageID
}
