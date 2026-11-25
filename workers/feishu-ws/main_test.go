package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

func TestSDKConfirmsOnlyAcceptedGatewayReceipts(t *testing.T) {
	connected := make(chan *websocket.Conn, 1)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == larkws.GenEndpointUri {
			_ = json.NewEncoder(writer).Encode(larkws.EndpointResp{
				Data: &larkws.Endpoint{
					Url:          "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?device_id=local&service_id=1",
					ClientConfig: &larkws.ClientConfig{PingInterval: 300},
				},
			})
			return
		}
		upgrader := websocket.Upgrader{}
		if conn, err := upgrader.Upgrade(writer, request, nil); err == nil {
			connected <- conn
		}
	}))
	defer server.Close()

	input, send := io.Pipe()
	output, publish := io.Pipe()
	defer send.Close()
	defer output.Close()
	finished := make(chan int, 1)
	go func() {
		finished <- run(input, publish)
		_ = publish.Close()
	}()
	if err := json.NewEncoder(send).Encode(config{AppID: "local", AppSecret: "fixture", Domain: server.URL}); err != nil {
		t.Fatal(err)
	}

	frames := make(chan frame, 8)
	go func() {
		defer close(frames)
		decoder := json.NewDecoder(output)
		for {
			var value frame
			if err := decoder.Decode(&value); err != nil {
				return
			}
			frames <- value
		}
	}()
	next := func(kind string) frame {
		t.Helper()
		timeout := time.NewTimer(5 * time.Second)
		defer timeout.Stop()
		for {
			select {
			case value, ok := <-frames:
				if !ok {
					t.Fatal("worker exited before writing", kind)
				}
				if value.Kind == kind {
					return value
				}
			case <-timeout.C:
				t.Fatal("worker did not write", kind)
			}
		}
	}
	next("ready")
	socket := <-connected
	defer socket.Close()
	encodedEvent := []byte(`{
		"schema":"2.0","header":{"event_type":"im.message.receive_v1","create_time":"123456789"},
		"event":{"message":{"message_id":"test-message","chat_type":"p2p","parent_id":"parent-message"},
		"sender":{"sender_type":"user","sender_id":{"open_id":"test-user"}}}
	}`)
	for _, accept := range []bool{true, false} {
		data := larkws.Frame{
			SeqID: 1, LogID: 1, Service: 1, Method: int32(larkws.FrameTypeData),
			Headers: []larkws.Header{{Key: larkws.HeaderType, Value: string(larkws.MessageTypeEvent)}},
			Payload: encodedEvent,
		}
		content, err := data.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if err := socket.WriteMessage(websocket.BinaryMessage, content); err != nil {
			t.Fatal(err)
		}
		message := next("message")
		if message.Payload == nil || message.Payload.CreateTime != "123456789" {
			t.Fatalf("missing event metadata: %+v", message)
		}
		if parent := message.Payload.Event.Message.ParentId; parent == nil || *parent != "parent-message" {
			t.Fatalf("missing parent message ID: %v", parent)
		}
		if err := json.NewEncoder(send).Encode(receipt{ID: message.ID, Accepted: accept}); err != nil {
			t.Fatal(err)
		}
		expected := http.StatusInternalServerError
		if accept {
			expected = http.StatusOK
		}
		_ = socket.SetReadDeadline(time.Now().Add(3 * time.Second))
		for {
			_, body, err := socket.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			var responseFrame larkws.Frame
			if err := responseFrame.Unmarshal(body); err != nil {
				t.Fatal(err)
			}
			if responseFrame.Method != int32(larkws.FrameTypeData) {
				continue
			}
			var response larkws.Response
			if err := json.Unmarshal(responseFrame.Payload, &response); err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != expected {
				t.Fatalf("got Feishu ACK %d, want %d", response.StatusCode, expected)
			}
			break
		}
	}
	_ = send.Close()
	select {
	case code := <-finished:
		if code != 0 {
			t.Fatalf("worker stopped with code %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop on parent pipe EOF")
	}
}
