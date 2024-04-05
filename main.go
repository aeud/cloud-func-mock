package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	endpoint           string
	agent              string
	authorizationToken string
	output             string
	callPath           string
	sessionPath        string
	secrets            map[string]interface{}
	customPayload      map[string]interface{}
	secretsStr         string
	customPayloadStr   string
	state              State
	stateStr           string
	testSetup          bool
	sessionID          string
	callID             string
)

func WriteState(state *State) {
	file, _ := os.Create(fmt.Sprintf("%s/state.json", sessionPath))
	file.Write(state.MarshalIndent())
	file.Close()
}
func ReadState() State {
	var s State
	file, _ := os.Open(fmt.Sprintf("%s/state.json", sessionPath))
	if err := json.NewDecoder(file).Decode(&s); err != nil {
		log.Println("Cannot read the state. Initializing to {}.")
		return NewState()
	}
	log.Printf("Current state is: \n%s\n", s.MarshalIndent())
	file.Close()
	return s
}

type CustomWriter struct {
	fileNumber int
	mut        *sync.Mutex
}

func (c *CustomWriter) Write(bs []byte) (int, error) {
	c.mut.Lock()
	c.Increment()
	file, err := os.Create(fmt.Sprintf("%s/log_%04d_%d.json", callPath, c.fileNumber, time.Now().Unix()))
	if err != nil {
		log.Fatalln(err)
	}
	c.mut.Unlock()
	defer file.Close()
	return file.Write(bs)
}
func (c *CustomWriter) Increment() {
	c.fileNumber++
}

func NewCustomWriter() *CustomWriter {
	return &CustomWriter{
		mut: new(sync.Mutex),
	}
}

type State map[string]interface{}

func (s State) Marshal() []byte {
	bs, _ := json.Marshal(s)
	return bs
}
func (s State) MarshalIndent() []byte {
	bs, _ := json.MarshalIndent(s, "", "  ")
	return bs
}

func NewState() State {
	return make(State)
}

type Response struct {
	State   State                    `json:"state"`
	Schema  map[string]interface{}   `json:"schema"`
	Insert  map[string][]interface{} `json:"insert"`
	Delete  map[string][]interface{} `json:"delete"`
	HasMore bool                     `json:"hasMore"`
}

func (r *Response) Marshal() []byte {
	bs, _ := json.Marshal(r)
	return bs
}

func (r *Response) MarshalIndent() []byte {
	bs, _ := json.MarshalIndent(r, "", "  ")
	return bs
}

func (r *Response) LogInsertions() string {
	results := make([]string, 0)
	for k, v := range r.Insert {
		if len(v) > 0 {
			bs, _ := json.Marshal(v[0])
			results = append(results, fmt.Sprintf("%d %s (ex: %s)", len(v), k, bs))
		}
	}
	return strings.Join(results, " - ")
}

type Request struct {
	Agent         string                 `json:"agent"`
	State         State                  `json:"state"`
	Secrets       map[string]interface{} `json:"secrets"`
	CustomPayload map[string]interface{} `json:"customPayload"`
	SetupTest     bool                   `json:"setup_test,omitempty"`
}

func NewRequest() *Request {
	return &Request{
		Agent:         agent,
		Secrets:       secrets,
		CustomPayload: customPayload,
		State:         NewState(),
	}
}
func NewRequestWithState(state State) *Request {
	r := NewRequest()
	r.State = state
	return r
}

func (req *Request) BuildSetupHttpRequest() (*http.Request, error) {
	return http.NewRequest("POST", endpoint, bytes.NewReader(req.Marshal()))
}

func (req *Request) SendRequest(w io.Writer) (*Response, error) {
	w.Write(req.MarshalIndent())
	client := http.Client{}
	httpRequest, err := req.BuildSetupHttpRequest()
	if err != nil {
		return nil, err
	}
	if authorizationToken != "" {
		httpRequest.Header.Add("Authorization", fmt.Sprintf("Bearer %s", authorizationToken))
	}
	httpRequest.Header.Add("Content-Type", "application/json")
	httpResponse, err := client.Do(httpRequest)
	if err != nil {
		return nil, err
	}
	if httpResponse.StatusCode != 200 {
		return nil, fmt.Errorf("http request answered a non 200 status code: %s", httpResponse.Status)
	}
	defer httpResponse.Body.Close()
	resp := &Response{}
	json.NewDecoder(httpResponse.Body).Decode(resp)
	return resp, nil
}

func (r *Request) Marshal() []byte {
	bs, _ := json.Marshal(r)
	return bs
}

func (r *Request) MarshalIndent() []byte {
	bs, _ := json.MarshalIndent(r, "", "  ")
	return bs
}

func SendSetupRequest() (*Response, error) {
	file := NewCustomWriter()
	request := NewRequest()
	request.SetupTest = true
	resp, err := request.SendRequest(file)
	if err != nil {
		return nil, err
	}
	file.Write(resp.MarshalIndent())

	return resp, err
}

func SendRequest(state State, w io.Writer) (*Response, error) {
	request := NewRequestWithState(state)
	resp, err := request.SendRequest(w)
	if err != nil {
		return nil, err
	}
	w.Write(resp.MarshalIndent())
	if resp.HasMore {
		WriteState(&resp.State)
		return SendRequest(resp.State, w)
	}
	WriteState(&resp.State)
	return resp, nil
}

func SendRequestWithState(state State) (*Response, error) {
	return SendRequest(state, NewCustomWriter())
}

func SenInitialdRequest() (*Response, error) {
	return SendRequest(NewState(), NewCustomWriter())
}

func init() {
	callID = fmt.Sprintf("call_%d", time.Now().Unix())
	// Read the cli inputs
	flag.StringVar(&endpoint, "endpoint", "http://localhost:8080", "Endpoint to send the requests to")
	flag.StringVar(&agent, "agent", "mock", "Agent to use in the request.")
	flag.StringVar(&authorizationToken, "token", "", "Authorization token to use in the request")
	flag.StringVar(&output, "output", "./output", "if a path is specified, files will be stored in the output directory. Otherwise standard output will be used")
	flag.StringVar(&sessionID, "session-id", fmt.Sprintf("session_%d", time.Now().Unix()), "session ID to use.")
	flag.StringVar(&secretsStr, "secrets", "{}", "secrets to use in the request")
	flag.StringVar(&customPayloadStr, "custom-payload", "{}", "custom payload. passed the custom payloads into your function every time we call the function.")
	flag.StringVar(&stateStr, "state", "", "state to send in the request")
	flag.BoolVar(&testSetup, "setup", false, "if mentioned, the test setup request will be sent to the endpoint (the one Fivetran uses when the connector is saved the first time.")
	flag.Parse()
	// Check if the input secrets is a json. If so, parse it.
	if err := json.Unmarshal([]byte(secretsStr), &secrets); err != nil {
		log.Fatalln(err)
	}
	// Check if the input customPayload is a json. If so, parse it.
	if err := json.Unmarshal([]byte(customPayloadStr), &customPayload); err != nil {
		log.Fatalln(err)
	}
	// Create the output folder if mentioned
	if output != "" {
		sessionPath = fmt.Sprintf("%s/%s", output, sessionID)
		callPath = fmt.Sprintf("%s/%s", sessionPath, callID)
		log.Printf("Session ID %s\n", sessionID)
		log.Printf("Call ID %s\n", callID)
		log.Printf("Writing files to %s\n", callPath)
		if err := os.MkdirAll(callPath, os.ModePerm); err != nil {
			log.Fatalln(err)
		}
	}
	// Check if the input state is a json. If so, parse it.
	if stateStr != "" {
		if err := json.Unmarshal([]byte(stateStr), &state); err != nil {
			log.Fatalln(err)
		}
	} else {
		state = ReadState()
	}
}

func main() {

	if testSetup {
		if _, err := SendSetupRequest(); err != nil {
			log.Fatalln(err)
		}
	} else {
		if _, err := SendRequestWithState(state); err != nil {
			log.Fatalln(err)
		}
	}
}
