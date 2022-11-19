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
	"time"
)

var (
	endpoint           string
	agent              string
	authorizationToken string
	output             string
	secrets            map[string]string
	secretsStr         string
	state              State
	stateStr           string
	testSetup          bool
)

type State map[string]interface{}

func (s State) Marshal() []byte {
	bs, _ := json.Marshal(s)
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
	Agent     string            `json:"agent"`
	State     State             `json:"state"`
	Secrets   map[string]string `json:"secrets"`
	SetupTest bool              `json:"setup_test,omitempty"`
}

func NewRequest() *Request {
	return &Request{
		Agent:   agent,
		Secrets: secrets,
		State:   NewState(),
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
	fmt.Fprintf(w, "Posting %s\n", req.Marshal())
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

func SendSetupRequest() (*Response, error) {
	file, err := GetOutput("setup")
	if err != nil {
		return nil, err
	}
	request := NewRequest()
	request.SetupTest = true
	resp, err := request.SendRequest(file)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(file, "Success. Response: %s\n", resp.Marshal())

	return resp, err
}

func SendRequest(state State, w io.Writer) (*Response, error) {
	request := NewRequestWithState(state)
	resp, err := request.SendRequest(w)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(w, "Inserted: %s\n", resp.LogInsertions())
	if resp.HasMore {
		fmt.Fprintf(w, "Intermediary state: %s\n", resp.State.Marshal())
		return SendRequest(resp.State, w)
	}
	fmt.Fprintf(w, "Final state: %s\n", resp.State.Marshal())
	return resp, nil
}

func SendRequestWithState(state State) (*Response, error) {
	file, err := GetOutput("request_with_state")
	if err != nil {
		return nil, err
	}
	return SendRequest(state, file)
}

func SenInitialdRequest() (*Response, error) {
	file, err := GetOutput("initial")
	if err != nil {
		return nil, err
	}
	return SendRequest(NewState(), file)
}

func GetOutput(fileName string) (io.Writer, error) {
	if output != "" {
		return os.Create(fmt.Sprintf("%s/%d_%s.txt", output, time.Now().Unix(), fileName))
	}
	return os.Stdout, nil
}

func init() {
	// Read the cli inputs
	flag.StringVar(&endpoint, "endpoint", "http://localhost:8080", "Endpoint to send the requests to")
	flag.StringVar(&agent, "agent", "mock", "Agent to use in the request.")
	flag.StringVar(&authorizationToken, "token", "", "Authorization token to use in the request")
	flag.StringVar(&output, "output", "", "if a path is specified, files will be stored in the output directory. Otherwise standard output will be used")
	flag.StringVar(&secretsStr, "secrets", "{}", "secrets to use in the request")
	flag.StringVar(&stateStr, "state", "{}", "state to send in the request")
	flag.BoolVar(&testSetup, "setup", false, "if mentioned, the test setup request will be sent to the endpoint (the one Fivetran uses when the connector is saved the first time.")
	flag.Parse()
	// Check if the input secrets is a json. If so, parse it.
	if err := json.Unmarshal([]byte(secretsStr), &secrets); err != nil {
		log.Fatalln(err)
	}
	// Check if the input state is a json. If so, parse it.
	if err := json.Unmarshal([]byte(stateStr), &state); err != nil {
		log.Fatalln(err)
	}
	// Create the output foler if mentioned
	if output != "" {
		if err := os.MkdirAll(output, os.ModePerm); err != nil {
			log.Println(err)
		}
	}
}

func main() {

	if testSetup {
		if _, err := SendSetupRequest(); err != nil {
			log.Println(err)
		}
	} else {
		if _, err := SendRequestWithState(state); err != nil {
			log.Println(err)
		}
	}
}
