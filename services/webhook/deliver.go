package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"code.gitea.io/gitea/models/db"
	webhook_model "code.gitea.io/gitea/models/webhook"
	"code.gitea.io/gitea/modules/graceful"
	"code.gitea.io/gitea/modules/hostcontainer"
	"code.gitea.io/gitea/modules/log"
	"code.gitea.io/gitea/modules/proxy"
	"code.gitea.io/gitea/modules/queue"
	"code.gitea.io/gitea/modules/setting"
	"code.gitea.io/gitea/modules/util"
	"code.gitea.io/gitea/modules/webhook"
)

// Deliver delivers a webhook task
func Deliver(ctx context.Context, t *webhook_model.WebhookTask) error {
	err := deliver(ctx, t)
	if err != nil {
		log.Error("Webhook delivery failed: %v", err)
	}
	return err
}

func parseRetryAfter(header string) time.Duration {
	if header == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(header); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if t, err := http.ParseTime(header); err == nil {
		return time.Until(t)
	}
	return 0
}

func deliver(ctx context.Context, t *webhook_model.WebhookTask) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	t.IsDelivered = true

	var req *http.Request
	// Mock client or actual HTTP client execution
	client := &http.Client{
		Timeout: 15 * time.Second,
	}

	req, err = http.NewRequestWithContext(ctx, "POST", t.URL, strings.NewReader(t.PayloadContent))
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		if retryAfter <= 0 {
			// Exponential backoff default starting at 5s
			retryAfter = 5 * time.Second
		}
		log.Warn("Rate limited by %s. Retry-After: %v", t.URL, retryAfter)
		// Requeue task with delay if supported, or return error to trigger queue retry
		return fmt.Errorf("rate limited: retry after %v", retryAfter)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("http status %d", resp.StatusCode)
	}

	return nil
}
