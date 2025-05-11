package webdav

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
)

func (h *Handler) handlePropfind(w http.ResponseWriter, r *http.Request) {
	// Setup context for metadata only
	ctx := context.WithValue(r.Context(), "metadataOnly", true)
	r = r.WithContext(ctx)

	// Determine depth (default "1")
	depth := r.Header.Get("Depth")
	if depth == "" {
		depth = "1"
	}

	cleanPath := path.Clean(r.URL.Path)

	// Build the list of entries
	type entry struct {
		href string
		fi   os.FileInfo
	}

	// Always include the resource itself
	f, err := h.OpenFile(r.Context(), cleanPath, os.O_RDONLY, 0)
	if err != nil {
		h.logger.Error().Err(err).Str("path", cleanPath).Msg("Failed to open file")
		http.NotFound(w, r)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to stat file")
		http.Error(w, "Server Error", http.StatusInternalServerError)
		return
	}

	// Collect children if a directory and depth allows
	children := make([]os.FileInfo, 0)
	if fi.IsDir() && depth != "0" {
		children = h.getChildren(cleanPath)
	}

	entries := make([]entry, 0, 1+len(children))
	entries = append(entries, entry{href: cleanPath, fi: fi})

	for _, child := range children {
		childHref := path.Join("/", cleanPath, child.Name())
		if child.IsDir() {
			childHref += "/"
		}
		entries = append(entries, entry{href: childHref, fi: child})
	}

	// Use a string builder for creating XML
	var sb strings.Builder

	// XML header and main element
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	sb.WriteString(`<d:multistatus xmlns:d="DAV:">`)

	// Format time once
	timeFormat := "2006-01-02T15:04:05.000-07:00"

	// Add responses for each entry
	for _, e := range entries {
		// Format href path properly
		u := &url.URL{Path: e.href}
		escaped := u.EscapedPath()

		sb.WriteString(`<d:response>`)
		sb.WriteString(fmt.Sprintf(`<d:href>%s</d:href>`, xmlEscape(escaped)))
		sb.WriteString(`<d:propstat>`)
		sb.WriteString(`<d:prop>`)

		// Resource type differs based on directory vs file
		if e.fi.IsDir() {
			sb.WriteString(`<d:resourcetype><d:collection/></d:resourcetype>`)
		} else {
			sb.WriteString(`<d:resourcetype/>`)
			sb.WriteString(fmt.Sprintf(`<d:getcontentlength>%d</d:getcontentlength>`, e.fi.Size()))
		}

		// Always add lastmodified and displayname
		lastModified := e.fi.ModTime().Format(timeFormat)
		sb.WriteString(fmt.Sprintf(`<d:getlastmodified>%s</d:getlastmodified>`, xmlEscape(lastModified)))
		sb.WriteString(fmt.Sprintf(`<d:displayname>%s</d:displayname>`, xmlEscape(e.fi.Name())))

		sb.WriteString(`</d:prop>`)
		sb.WriteString(`<d:status>HTTP/1.1 200 OK</d:status>`)
		sb.WriteString(`</d:propstat>`)
		sb.WriteString(`</d:response>`)
	}

	// Close root element
	sb.WriteString(`</d:multistatus>`)

	// Set headers
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Vary", "Accept-Encoding")

	// Set status code and write response
	w.WriteHeader(http.StatusMultiStatus) // 207 MultiStatus
	_, _ = w.Write([]byte(sb.String()))
}

// Basic XML escaping function
func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	s = strings.ReplaceAll(s, "\n", "&#10;")
	s = strings.ReplaceAll(s, "\r", "&#13;")
	s = strings.ReplaceAll(s, "\t", "&#9;")
	return s
}
