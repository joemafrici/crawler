package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/temoto/robotstxt"
	"golang.org/x/net/html"
	"golang.org/x/time/rate"
)

type CrawlResult struct {
	URL        string    `json:"url"`
	Timestamp  time.Time `json:"timestamp"`
	PlatoCount int       `json:"plato_count"`
	Content    []string  `json:"content"`
	Title      string    `json:"title"`
}

const (
	maxDepth        = 3
	maxExtract      = 150
	errLogFilename  = "err.log"
	resultsFilename = "results.json"

	requestsPerSecond = 2
	burstLimit        = 5

	numWorkers    = 5
	queueCapacity = 10000000

	saveInterval    = 30 * time.Second
	visitedURLsFile = "visited_urls.json"
	urlQueueFile    = "urlqueue.json"
)

var (
	crawlResults   []CrawlResult
	crawlResultsMu sync.Mutex

	errFileLogger     *log.Logger
	resultsFileLogger *log.Logger
	errLogFile        *os.File
	resultsFile       *os.File

	limiter *rate.Limiter

	robotsData map[string]*robotstxt.RobotsData
	robotsMu   sync.Mutex

	visitedURLs sync.Map
	urlQueue    chan string
)

// ***********************************************
// init gets called before main
func init() {
	var err error
	errLogFile, err = os.OpenFile(errLogFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	resultsFile, err = os.OpenFile(resultsFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	errFileLogger = log.New(errLogFile, "", log.LstdFlags)
	//resultsFileLogger = log.New(resultsFile, "", log.LstdFlags)

	limiter = rate.NewLimiter(rate.Limit(requestsPerSecond), burstLimit)

	robotsData = make(map[string]*robotstxt.RobotsData)
	urlQueue = make(chan string, queueCapacity)
}

// ***********************************************
func saveVisitedURLs() error {
	visitedURLsSlice := make([]string, 0)
	visitedURLs.Range(func(key, value interface{}) bool {
		visitedURLsSlice = append(visitedURLsSlice, key.(string))
		return true
	})

	data, err := json.Marshal(visitedURLsSlice)
	if err != nil {
		return err
	}

	return os.WriteFile(visitedURLsFile, data, 0644)
}

// ***********************************************
func loadVisitedURLs() error {
	data, err := os.ReadFile(visitedURLsFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var visitedURLsSlice []string
	if err := json.Unmarshal(data, &visitedURLsSlice); err != nil {
		return nil
	}

	for _, url := range visitedURLsSlice {
		visitedURLs.Store(url, true)
	}
	return nil
}

// ***********************************************
func saveURLQueue() error {
	queueSlice := make([]string, 0, len(urlQueue))
	for len(urlQueue) > 0 {
		queueSlice = append(queueSlice, <-urlQueue)
	}
}

// ***********************************************
func saveResult(result CrawlResult) {
	crawlResultsMu.Lock()
	defer crawlResultsMu.Unlock()
	crawlResults = append(crawlResults, result)
}

// ***********************************************
func saveResultToFile(result CrawlResult) error {
	encoder := json.NewEncoder(resultsFile)
	encoder.SetIndent("", " ")
	return encoder.Encode(result)
}

// ***********************************************
func getRobotsTxt(baseURL *url.URL) (*robotstxt.RobotsData, error) {
	robotsMu.Lock()
	defer robotsMu.Unlock()

	host := baseURL.Host
	if data, ok := robotsData[host]; ok {
		return data, nil
	}

	robotsURL := baseURL.Scheme + "://" + baseURL.Host + "/robots.txt"
	resp, err := http.Get(robotsURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := robotstxt.FromResponse(resp)
	if err != nil {
		return nil, err
	}

	robotsData[host] = data
	return data, nil
}

// ***********************************************
func isAllowed(URL string) bool {
	parsedURL, err := url.Parse(URL)
	if err != nil {
		logError("Error parsing URL %s: %v", URL, err)
		return false
	}

	robotsTxt, err := getRobotsTxt(parsedURL)
	if err != nil {
		logError("Error fetching robots.txt for %s: %v", parsedURL.Host, err)
		return true
	}

	return robotsTxt.TestAgent(parsedURL.Path, "gojo")
}

// ***********************************************
func logError(format string, v ...interface{}) {
	errFileLogger.Printf(format, v...)
}

// ***********************************************
func logOut(format string, v ...interface{}) {
	resultsFileLogger.Printf(format, v...)
}

// ***********************************************
func fetch(urlStr string) (*html.Node, error) {
	if err := limiter.Wait(context.Background()); err != nil {
		return nil, fmt.Errorf("rate limiter error: %v", err)
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("unsupported URL scheme: %s", u.Scheme)
	}

	resp, err := http.Get(urlStr)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error: %s", resp.Status)
	}

	return html.Parse(resp.Body)
}

// ***********************************************
func extractText(n *html.Node) string {
	if n.Type == html.TextNode {
		return n.Data
	}

	var result string
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		result += extractText(c)
	}

	return result
}

// ***********************************************
func extractSentence(text string, word string, maxChars int) string {
	lowercaseText := strings.ToLower(text)
	wordIndex := strings.Index(lowercaseText, word)
	if wordIndex == -1 {
		return ""
	}

	start := wordIndex
	for start > 0 && !isSentenceEnd(rune(text[start-1])) {
		start--
	}

	end := wordIndex + len(word)
	for end < len(text) && !isSentenceEnd(rune(text[end])) {
		end++
	}

	if wordIndex-start > maxChars {
		start = wordIndex - maxChars
	}

	if end-(wordIndex+len(word)) > maxChars {
		end = wordIndex + len(word) + maxChars

	}

	return strings.TrimSpace(text[start:end])
}

// ***********************************************
func isSentenceEnd(r rune) bool {
	return r == '.' || r == '!' || r == '?'
}

// ***********************************************
// crawls baseURL and returns any links it finds and
// any sentence fragments containing "plato"
func crawl(baseURL *url.URL, doc *html.Node) ([]string, []string, string, int) {
	log.Printf("crawling %s", baseURL)
	var links []string
	var contentStrs []string
	var platoCount int
	var title string

	var f func(*html.Node)
	f = func(n *html.Node) {
		if n == nil {
			return
		}
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					u, err := baseURL.Parse(attr.Val)
					if err != nil {
						continue
					}
					if u.Scheme != "http" && u.Scheme != "https" {
						continue
					}
					link := u.String()
					links = append(links, link)
				}
			}
		} else if n.Type == html.ElementNode && n.Data == "title" {
			if n.FirstChild != nil {
				title = n.FirstChild.Data
			}
		} else if n.Type == html.TextNode {
			text := extractText(n)
			if strings.Contains(strings.ToLower(text), "plato") {
				sentence := extractSentence(text, "plato", maxExtract)
				if sentence != "" {
					contentStrs = append(contentStrs, sentence)
					log.Println("found Plato..")
					platoCount++
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)
	return links, contentStrs, title, platoCount
}

// ***********************************************
func worker(results chan<- CrawlResult, wg *sync.WaitGroup) {
	log.Println("worker spawned")
	defer wg.Done()

	for urlStr := range urlQueue {
		if _, visited := visitedURLs.LoadOrStore(urlStr, true); visited {
			continue
		}

		if !isAllowed(urlStr) {
			logError("URL not allowed by robots.txt: %s", urlStr)
			continue
		}

		baseURL, err := url.Parse(urlStr)
		if err != nil {
			logError("Error parsing URL %s: %v", urlStr, err)
		}

		doc, err := fetch(urlStr)
		if err != nil {
			logError("Error fetching URL %s: %v", urlStr, err)
		}

		links, contentStrs, title, platoCount := crawl(baseURL, doc)

		results <- CrawlResult{
			URL:        urlStr,
			Timestamp:  time.Now(),
			PlatoCount: platoCount,
			Content:    contentStrs,
			Title:      title,
		}

		for _, link := range links {
			select {
			case urlQueue <- link:
			default:
				logError("URL queue is full, discarding URL: %s", link)
			}
		}
	}
}

// ***********************************************
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: crawler <seed_url1> [seed_url2] ...")
		os.Exit(1)
	}
	seedURLs := os.Args[1:]

	results := make(chan CrawlResult)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		log.Println("spawning worker")
		go worker(results, &wg)
	}

	for _, url := range seedURLs {
		log.Printf("adding %s to queue..\n", url)
		urlQueue <- url
	}

	go func() {
		wg.Wait()
		close(urlQueue)
		close(results)
	}()

	for result := range results {
		if result.PlatoCount > 0 {
			err := saveResultToFile(result)
			if err != nil {
				logError("Error saving result to file: %v", err)
			}
		}
	}

	fmt.Println("Crawling complete. Check crawler.log for any errors")
}
