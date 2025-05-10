package debrid

import (
	"encoding/xml"
	"fmt"
	"github.com/beevik/etree"
	"github.com/sirrobot01/decypharr/internal/request"
	"net/http"
	"os"
	"path"
	"sync"
	"time"
)

const (
	DavNS = "DAV:"
)

// Multistatus XML types for WebDAV response
type Multistatus struct {
	XMLName   xml.Name   `xml:"D:multistatus"`
	Namespace string     `xml:"xmlns:D,attr"`
	Responses []Response `xml:"D:response"`
}

type Response struct {
	Href     string   `xml:"D:href"`
	Propstat Propstat `xml:"D:propstat"`
}

type Propstat struct {
	Prop   Prop   `xml:"D:prop"`
	Status string `xml:"D:status"`
}

type Prop struct {
	ResourceType  ResourceType  `xml:"D:resourcetype"`
	DisplayName   string        `xml:"D:displayname"`
	LastModified  string        `xml:"D:getlastmodified"`
	ContentType   string        `xml:"D:getcontenttype"`
	ContentLength string        `xml:"D:getcontentlength"`
	SupportedLock SupportedLock `xml:"D:supportedlock"`
}

type ResourceType struct {
	Collection *struct{} `xml:"D:collection,omitempty"`
}

type SupportedLock struct {
	LockEntry LockEntry `xml:"D:lockentry"`
}

type LockEntry struct {
	LockScope LockScope `xml:"D:lockscope"`
	LockType  LockType  `xml:"D:locktype"`
}

type LockScope struct {
	Exclusive *struct{} `xml:"D:exclusive"`
}

type LockType struct {
	Write *struct{} `xml:"D:write"`
}

func (c *Cache) refreshParentXml() error {
	// Refresh the defaults first
	parents := []string{"__all__", "torrents"}
	torrents := c.GetListing("__all__")
	clientName := c.client.GetName()
	customFolders := c.GetCustomFolders()
	wg := sync.WaitGroup{}
	totalFolders := len(parents) + len(customFolders)
	wg.Add(totalFolders)
	errCh := make(chan error, totalFolders)
	for _, parent := range parents {
		parent := parent
		go func() {
			defer wg.Done()
			if err := c.refreshFolderXml(torrents, clientName, parent); err != nil {
				errCh <- fmt.Errorf("failed to refresh folder %s: %v", parent, err)
			}
		}()
	}
	// refresh custom folders
	for _, folder := range customFolders {
		go func() {
			folder := folder
			defer wg.Done()
			listing := c.GetListing(folder)
			if err := c.refreshFolderXml(listing, clientName, folder); err != nil {
				errCh <- fmt.Errorf("failed to refresh folder %s: %v", folder, err)
			}
		}()

	}
	wg.Wait()
	close(errCh)
	// if any errors, return the first
	if err := <-errCh; err != nil {
		return err
	}
	return nil
}

func (c *Cache) refreshFolderXml(torrents []os.FileInfo, clientName, parent string) error {
	// Get the current timestamp in RFC1123 format
	currentTime := time.Now().UTC().Format(http.TimeFormat)

	// Create the multistatus response structure
	ms := Multistatus{
		Namespace: DavNS,
		Responses: make([]Response, 0, len(torrents)+1), // Pre-allocate for parent + torrents
	}

	// Add the parent directory
	baseUrl := path.Join("webdav", clientName, parent)

	// Add parent response
	ms.Responses = append(ms.Responses, createDirectoryResponse(baseUrl, parent, currentTime))

	// Add torrents to the response
	for _, torrent := range torrents {
		name := torrent.Name()
		torrentPath := path.Join("/webdav", clientName, parent, name) + "/"
		ms.Responses = append(ms.Responses, createDirectoryResponse(torrentPath, name, currentTime))
	}

	// Create a buffer and encode the XML
	xmlData, err := xml.MarshalIndent(ms, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to generate XML: %v", err)
	}

	// Add XML declaration
	xmlHeader := []byte(xml.Header)
	xmlOutput := append(xmlHeader, xmlData...)

	// Cache the result
	cacheKey := fmt.Sprintf("%s:1", baseUrl)

	// Assume Gzip function exists elsewhere
	gzippedData := request.Gzip(xmlOutput) // Replace with your actual gzip function

	c.PropfindResp.Store(cacheKey, PropfindResponse{
		Data:        xmlOutput,
		GzippedData: gzippedData,
		Ts:          time.Now(),
	})

	return nil
}

func createDirectoryResponse(href, displayName, modTime string) Response {
	return Response{
		Href: href,
		Propstat: Propstat{
			Prop: Prop{
				ResourceType: ResourceType{
					Collection: &struct{}{},
				},
				DisplayName:   displayName,
				LastModified:  modTime,
				ContentType:   "httpd/unix-directory",
				ContentLength: "0",
				SupportedLock: SupportedLock{
					LockEntry: LockEntry{
						LockScope: LockScope{
							Exclusive: &struct{}{},
						},
						LockType: LockType{
							Write: &struct{}{},
						},
					},
				},
			},
			Status: "HTTP/1.1 200 OK",
		},
	}
}

func addDirectoryResponse(multistatus *etree.Element, href, displayName, modTime string) *etree.Element {
	responseElem := multistatus.CreateElement("D:response")

	// Add href - ensure it's properly formatted
	hrefElem := responseElem.CreateElement("D:href")
	hrefElem.SetText(href)

	// Add propstat
	propstatElem := responseElem.CreateElement("D:propstat")

	// Add prop
	propElem := propstatElem.CreateElement("D:prop")

	// Add resource type (collection = directory)
	resourceTypeElem := propElem.CreateElement("D:resourcetype")
	resourceTypeElem.CreateElement("D:collection")

	// Add display name
	displayNameElem := propElem.CreateElement("D:displayname")
	displayNameElem.SetText(displayName)

	// Add last modified time
	lastModElem := propElem.CreateElement("D:getlastmodified")
	lastModElem.SetText(modTime)

	// Add content type for directories
	contentTypeElem := propElem.CreateElement("D:getcontenttype")
	contentTypeElem.SetText("httpd/unix-directory")

	// Add length (size) - directories typically have zero size
	contentLengthElem := propElem.CreateElement("D:getcontentlength")
	contentLengthElem.SetText("0")

	// Add supported lock
	lockElem := propElem.CreateElement("D:supportedlock")
	lockEntryElem := lockElem.CreateElement("D:lockentry")

	lockScopeElem := lockEntryElem.CreateElement("D:lockscope")
	lockScopeElem.CreateElement("D:exclusive")

	lockTypeElem := lockEntryElem.CreateElement("D:locktype")
	lockTypeElem.CreateElement("D:write")

	// Add status
	statusElem := propstatElem.CreateElement("D:status")
	statusElem.SetText("HTTP/1.1 200 OK")

	return responseElem
}
