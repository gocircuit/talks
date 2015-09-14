package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/gocircuit/circuit/client"
)

// Http wraps a controller and provides HTTP handlers for the AddJob and String methods of the controller.
type Http struct {
	controller *Controller
}

func (h Http) ShowStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, h.controller.String())
}

func (h Http) AddJob(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var jobSpec JobSpec
	if err := json.NewDecoder(r.Body).Decode(&jobSpec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	name, cmd := jobSpec.Cmd()
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := h.controller.AddJob(name, cmd); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type JobSpec struct {
	Name string   `json:"name"`
	Env  []string `json:"env"`
	Dir  string   `json:"dir"`
	Path string   `json:"path"`
	Args []string `json:"args"`
}

func (js *JobSpec) Cmd() (name string, cmd client.Cmd) {
	return strings.TrimSpace(js.Name),
		client.Cmd{
			Env:  js.Env,
			Dir:  js.Dir,
			Path: js.Path,
			Args: js.Args,
		}
}
