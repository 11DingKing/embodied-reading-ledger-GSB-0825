package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"embodied-reading-ledger/internal/service"
)

const contentTypeJSON = "application/json; charset=utf-8"

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write response body: %v", err)
	}
}

func writeBytes(w http.ResponseWriter, status int, b []byte) {
	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func writeAPIError(w http.ResponseWriter, err error) {
	var apiErr *service.APIError
	if errors.As(err, &apiErr) {
		writeJSON(w, apiErr.Status, service.ErrorBody{
			Error: service.ErrorBodyInner{
				Code:    apiErr.Code,
				Message: apiErr.Message,
				Details: apiErr.Details,
			},
		})
		return
	}
	writeJSON(w, http.StatusInternalServerError, service.ErrorBody{
		Error: service.ErrorBodyInner{
			Code:    service.CodeInternal,
			Message: "internal server error",
		},
	})
}
