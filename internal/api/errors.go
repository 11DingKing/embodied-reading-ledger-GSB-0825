package api

import (
	"encoding/json"
	"log"
	"net/http"
)

type APIError struct {
	Status  int
	Code    string
	Message string
	Details map[string]any
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func writeAPIError(w http.ResponseWriter, e *APIError) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(e.Status)
	_ = json.NewEncoder(w).Encode(errorBody{
		Error: errorDetail{Code: e.Code, Message: e.Message, Details: e.Details},
	})
}

func validationError(message string, details map[string]any) *APIError {
	return &APIError{Status: http.StatusBadRequest, Code: "VALIDATION_ERROR", Message: message, Details: details}
}

func internalError(w http.ResponseWriter, where string, err error) {
	log.Printf("%s: %v", where, err)
	writeAPIError(w, &APIError{
		Status:  http.StatusInternalServerError,
		Code:    "INTERNAL",
		Message: "internal server error",
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) []byte {
	body := writeJSONBuffer(v)
	if body == nil {
		return nil
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
	return body
}

func writeJSONBuffer(v any) []byte {
	body, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return body
}

func writeReplay(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Idempotent-Replay", "true")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
