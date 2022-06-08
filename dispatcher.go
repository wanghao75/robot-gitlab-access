package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/opensourceways/community-robot-lib/gitlabclient"
	"github.com/xanzy/go-gitlab"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	noteableTypeCommit       = "Commit"
	noteableTypeIssue        = "Issue"
	noteableTypeMergeRequest = "MergeRequest"
)

type noteEvent struct {
	ObjectKind       string `json:"object_kind"`
	ObjectAttributes struct {
		NoteableType string `json:"noteable_type"`
	} `json:"object_attributes"`
}

type dispatcher struct {
	agent *demuxConfigAgent

	hmac func() string

	// ec is an http client used for dispatching events
	// to external plugin services.
	ec http.Client
	// Tracks running handlers for graceful shutdown
	wg sync.WaitGroup
}

func (d *dispatcher) wait() {
	d.wg.Wait() // Handle remaining requests
}

// ServeHTTP validates an incoming webhook and puts it into the event channel.
func (d *dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	eventType, eventGUID, _, payload, ok, _ := gitlabclient.ValidateWebhook(w, r, d.hmac)
	if !ok {
		return
	}
	fmt.Fprint(w, "Event received. Have a nice day.")

	l := logrus.WithFields(
		logrus.Fields{
			"event-type": eventType,
			"event-id":   eventGUID,
		},
	)

	if err := d.dispatch(eventType, payload, r.Header, l); err != nil {
		l.WithError(err).Error()
	}
}

func (d *dispatcher) dispatch(eventType string, payload []byte, h http.Header, l *logrus.Entry) error {
	org := ""
	repo := ""

	et := gitlab.EventType(eventType)
	switch et {
	case gitlab.EventTypeMergeRequest:
		e, err := gitlabclient.ConvertToMergeEvent(payload)
		if err != nil {
			return err
		}

		org, repo = gitlabclient.GetOrgRepo(e.Project.PathWithNamespace)

	case gitlab.EventTypeIssue:
		e, err := gitlabclient.ConvertToIssueEvent(payload)
		if err != nil {
			return err
		}

		org, repo = gitlabclient.GetOrgRepo(e.Project.PathWithNamespace)

	case gitlab.EventTypeNote:
		note := &noteEvent{}
		err := json.Unmarshal(payload, note)
		if err != nil {
			return err
		}

		if note.ObjectKind != string(gitlab.NoteEventTargetType) {
			return nil
		}

		switch note.ObjectAttributes.NoteableType {
		case noteableTypeCommit:
			e, err := gitlabclient.ConvertToCommitCommentEvent(payload)
			if err != nil {
				return err
			}
			org, repo = gitlabclient.GetOrgRepo(e.Project.PathWithNamespace)
		case noteableTypeMergeRequest:
			e, err := gitlabclient.ConvertToMergeCommentEvent(payload)
			if err != nil {
				return err
			}

			org, repo = gitlabclient.GetOrgRepo(e.Project.PathWithNamespace)
		case noteableTypeIssue:
			e, err := gitlabclient.ConvertToIssueCommentEvent(payload)
			if err != nil {
				return err
			}

			org, repo = gitlabclient.GetOrgRepo(e.Project.PathWithNamespace)

		default:
			return fmt.Errorf("unexpected noteable type %s", note.ObjectAttributes.NoteableType)
		}

	case gitlab.EventTypePush:
		e, err := gitlabclient.ConvertToPushEvent(payload)
		if err != nil {
			return err
		}

		org, repo = gitlabclient.GetOrgRepo(e.Project.PathWithNamespace)

	default:
		l.Debug("Ignoring unknown event type")
		return fmt.Errorf("unexpected event type: %s", eventType)
	}

	l = l.WithFields(logrus.Fields{
		"org":  org,
		"repo": repo,
	})

	endpoints := d.agent.getEndpoints(org, repo, eventType)
	l.WithField("endpoints", strings.Join(endpoints, ", ")).Info("start dispatching event.")

	d.doDispatch(endpoints, payload, h, l)
	return nil
}

func (d *dispatcher) doDispatch(endpoints []string, payload []byte, h http.Header, l *logrus.Entry) {
	h.Set("User-Agent", "Robot-Gitlab-Access")

	newReq := func(endpoint string) (*http.Request, error) {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(payload))
		if err != nil {
			return nil, err
		}

		req.Header = h

		return req, nil
	}

	reqs := make([]*http.Request, 0, len(endpoints))

	for _, endpoint := range endpoints {
		if req, err := newReq(endpoint); err == nil {
			reqs = append(reqs, req)
		} else {
			l.WithError(err).WithField("endpoint", endpoint).Error("Error generating http request.")
		}
	}

	for _, req := range reqs {
		d.wg.Add(1)

		// concurrent action is sending request not generating it.
		// so, generates requests first.
		go func(req *http.Request) {
			defer d.wg.Done()

			if err := d.forwardTo(req); err != nil {
				l.WithError(err).WithField("endpoint", req.URL.String()).Error("Error forwarding event.")
			}
		}(req)
	}
}

func (d *dispatcher) forwardTo(req *http.Request) error {
	resp, err := d.do(req)
	if err != nil || resp == nil {
		return err
	}

	defer resp.Body.Close()

	rb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("response has status %q and body %q", resp.Status, string(rb))
	}
	return nil
}

func (d *dispatcher) do(req *http.Request) (resp *http.Response, err error) {
	if resp, err = d.ec.Do(req); err == nil {
		return
	}

	maxRetries := 4
	backoff := 100 * time.Millisecond

	for retries := 0; retries < maxRetries; retries++ {
		time.Sleep(backoff)
		backoff *= 2

		if resp, err = d.ec.Do(req); err == nil {
			break
		}
	}
	return
}
