package service

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"

	"embodied-reading-ledger/internal/domain"
	"embodied-reading-ledger/internal/store"

	"github.com/jackc/pgx/v5"
)

var uuidRegex = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func isValidUUID(s string) bool { return uuidRegex.MatchString(s) }

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return strings.TrimSpace(*s)
}

// --- Create book ---

type CreateBookRequest struct {
	ISBN          *string            `json:"isbn"`
	Title         string             `json:"title"`
	Author        string             `json:"author"`
	Publisher     *string            `json:"publisher"`
	PublishedYear *int               `json:"publishedYear"`
	TotalPages    *int               `json:"totalPages"`
	Format        *domain.BookFormat `json:"format"`
}

func (s *Service) CreateBook(ctx context.Context, req CreateBookRequest, idemKey string, reqHash []byte) (int, []byte, error) {
	// Normalize
	req.Title = strings.TrimSpace(req.Title)
	req.Author = strings.TrimSpace(req.Author)
	if req.ISBN != nil {
		v := strings.TrimSpace(*req.ISBN)
		req.ISBN = &v
	}
	if req.Publisher != nil {
		v := strings.TrimSpace(*req.Publisher)
		req.Publisher = &v
	}
	format := domain.FormatPaperback
	if req.Format != nil {
		format = *req.Format
	}

	return s.writeTxn(ctx, http.MethodPost, "/books", idemKey, reqHash,
		func(ctx context.Context, tx pgx.Tx) (int, any, *APIError) {
			if req.Title == "" {
				return 0, nil, errValidation("title is required", map[string]any{"field": "title"})
			}
			if req.Author == "" {
				return 0, nil, errValidation("author is required", map[string]any{"field": "author"})
			}
			if !format.Valid() {
				return 0, nil, errValidation("invalid format", map[string]any{"field": "format", "value": string(format)})
			}
			if req.PublishedYear != nil && *req.PublishedYear <= 0 {
				return 0, nil, errValidation("publishedYear must be positive", map[string]any{"field": "publishedYear"})
			}
			if req.TotalPages != nil && *req.TotalPages <= 0 {
				return 0, nil, errInvalidPage("totalPages must be positive")
			}
			b, err := store.New(tx).InsertBook(ctx, store.InsertBookParams{
				ISBN:          req.ISBN,
				Title:         req.Title,
				Author:        req.Author,
				Publisher:     req.Publisher,
				PublishedYear: req.PublishedYear,
				TotalPages:    req.TotalPages,
				Format:        format,
			})
			if err != nil {
				return 0, nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
			}
			return http.StatusCreated, b, nil
		})
}

// --- Create session ---

type CreateSessionRequest struct {
	BookID string  `json:"bookId"`
	Label  *string `json:"label"`
}

func (s *Service) CreateSession(ctx context.Context, req CreateSessionRequest, idemKey string, reqHash []byte) (int, []byte, error) {
	req.BookID = strings.TrimSpace(req.BookID)
	if req.Label != nil {
		v := strings.TrimSpace(*req.Label)
		req.Label = &v
	}
	path := "/sessions"

	return s.writeTxn(ctx, http.MethodPost, path, idemKey, reqHash,
		func(ctx context.Context, tx pgx.Tx) (int, any, *APIError) {
			if !isValidUUID(req.BookID) {
				return 0, nil, errValidation("bookId must be a valid UUID", map[string]any{"field": "bookId"})
			}
			_, err := store.New(tx).GetBook(ctx, req.BookID)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					return 0, nil, errBookNotFound()
				}
				return 0, nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
			}
			sess, err := store.New(tx).InsertSession(ctx, store.InsertSessionParams{
				BookID: req.BookID,
				Label:  req.Label,
			})
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					return 0, nil, errBookNotFound()
				}
				return 0, nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
			}
			return http.StatusCreated, sess, nil
		})
}
