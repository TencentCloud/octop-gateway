package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

type config struct {
	AppID             string `json:"app_id"`
	AppSecret         string `json:"app_secret"`
	VerificationToken string `json:"verification_token"`
	EncryptKey        string `json:"encrypt_key"`
	Domain            string `json:"domain"`
}

type frame struct {
	Kind    string   `json:"kind"`
	ID      string   `json:"id,omitempty"`
	Payload *message `json:"payload,omitempty"`
}

type message struct {
	Event      *larkim.P2MessageReceiveV1Data `json:"event"`
	CreateTime string                         `json:"create_time"`
}

type receipt struct {
	ID       string `json:"id"`
	Accepted bool   `json:"accepted"`
}

type gatewayLink struct {
	encoder *json.Encoder
	writeMu sync.Mutex

	mu      sync.Mutex
	pending map[string]chan bool
	nextID  uint64
}

func (link *gatewayLink) send(value frame) error {
	link.writeMu.Lock()
	defer link.writeMu.Unlock()
	return link.encoder.Encode(value)
}

func (link *gatewayLink) readReceipts(decoder *json.Decoder, cancel context.CancelFunc) {
	defer cancel()
	for {
		var ack receipt
		if err := decoder.Decode(&ack); err != nil {
			return
		}
		link.mu.Lock()
		reply := link.pending[ack.ID]
		if reply != nil {
			delete(link.pending, ack.ID)
			reply <- ack.Accepted
		}
		link.mu.Unlock()
	}
}

func (link *gatewayLink) deliver(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
	if event == nil || event.Event == nil || event.Event.Message == nil || event.Event.Sender == nil {
		return errors.New("incomplete message event")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	link.mu.Lock()
	if len(link.pending) >= 64 {
		link.mu.Unlock()
		return errors.New("gateway receipt capacity reached")
	}
	link.nextID++
	id := strconv.FormatUint(link.nextID, 10)
	reply := make(chan bool, 1)
	link.pending[id] = reply
	link.mu.Unlock()
	defer func() {
		link.mu.Lock()
		delete(link.pending, id)
		link.mu.Unlock()
	}()

	payload := &message{Event: event.Event}
	if event.EventV2Base != nil && event.EventV2Base.Header != nil {
		payload.CreateTime = event.EventV2Base.Header.CreateTime
	}
	if err := link.send(frame{Kind: "message", ID: id, Payload: payload}); err != nil {
		return err
	}
	select {
	case accepted := <-reply:
		if !accepted {
			return errors.New("gateway did not accept message")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func run(input io.Reader, output io.Writer) int {
	decoder := json.NewDecoder(input)
	var settings config
	if err := decoder.Decode(&settings); err != nil || settings.AppID == "" || settings.AppSecret == "" {
		return 2
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	link := &gatewayLink{encoder: json.NewEncoder(output), pending: make(map[string]chan bool)}

	// Closing the parent pipe also shuts down the worker after a gateway crash.
	go link.readReceipts(decoder, cancel)
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		<-ctx.Done()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-finished:
		case <-timer.C:
			os.Exit(1)
		}
	}()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := link.send(frame{Kind: "heartbeat"}); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	handler := dispatcher.NewEventDispatcher(settings.VerificationToken, settings.EncryptKey).
		OnP2MessageReceiveV1(link.deliver).
		OnP2MessageReactionCreatedV1(func(context.Context, *larkim.P2MessageReactionCreatedV1) error { return nil }).
		OnP2MessageReactionDeletedV1(func(context.Context, *larkim.P2MessageReactionDeletedV1) error { return nil })
	defaults := []larkws.ClientOption{
		larkws.WithEventHandler(handler),
		larkws.WithLogger(quietLogger{}),
		larkws.WithLogLevel(larkcore.LogLevelError),
		// The gateway owns all retries, including the limit across applications.
		larkws.WithAutoReconnect(false),
		larkws.WithOnReady(func() {
			if err := link.send(frame{Kind: "ready"}); err != nil {
				cancel()
			}
		}),
	}
	if settings.Domain != "" {
		defaults = append(defaults, larkws.WithDomain(settings.Domain))
	}
	client := larkws.NewClient(settings.AppID, settings.AppSecret, defaults...)
	if err := client.Start(ctx); err != nil && ctx.Err() == nil {
		var invalid *larkws.ClientError
		if errors.As(err, &invalid) && (invalid.Code == larkws.AuthFailed || invalid.Code == larkws.Forbidden) {
			_ = link.send(frame{Kind: "rejected"})
			return 2
		}
		return 1
	}
	return 0
}

// stdout carries only protocol frames. SDK logs can include event content.
type quietLogger struct{}

func (quietLogger) Debug(context.Context, ...interface{}) {}
func (quietLogger) Info(context.Context, ...interface{})  {}
func (quietLogger) Warn(context.Context, ...interface{})  {}
func (quietLogger) Error(context.Context, ...interface{}) {}

func main() {
	protocol := os.Stdout
	// The SDK's dispatcher prints a startup line before a logger can be set.
	os.Stdout = os.Stderr
	os.Exit(run(os.Stdin, protocol))
}
