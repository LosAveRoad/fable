// Package httpdocs generates OpenAPI 3.1 from the same typed operations used to
// mount the HTTP routes. It deliberately does not infer business error branches.
package httpdocs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type ErrorBody struct {
	Error string `json:"error"`
}

// RawError describes historical c.JSON(status, err) responses. Errors with no
// exported fields encode as {}, while validator errors encode as an array.
type RawError struct{}
type Response struct {
	Status      int
	Description string
	Bodies      []any
}
type Route struct {
	Method, Path, ID, Summary string
	Authenticated             bool
	Handler                   gin.HandlerFunc
	Request                   any
	Query                     any
	// RequiredFields covers explicit controller checks that are not binding tags.
	RequiredFields []string
	QueryRules     map[string]map[string]any
	Responses      []Response
}

func schemaType(t reflect.Type, request bool, seen map[reflect.Type]bool) map[string]any {
	if t == reflect.TypeOf(RawError{}) {
		return map[string]any{"anyOf": []any{map[string]any{"type": "object"}, map[string]any{"type": "array", "items": map[string]any{"type": "object"}}}}
	}
	if t == reflect.TypeOf(time.Time{}) {
		return map[string]any{"type": "string", "format": "date-time"}
	}
	if t.Kind() == reflect.Pointer {
		return map[string]any{"anyOf": []any{schemaType(t.Elem(), request, seen), map[string]any{"type": "null"}}}
	}
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": schemaType(t.Elem(), request, seen)}
	case reflect.Struct:
		if seen[t] {
			panic("recursive HTTP DTO: " + t.String())
		}
		seen[t] = true
		defer delete(seen, t)
		properties := map[string]any{}
		required := []string{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			tag := strings.Split(f.Tag.Get("json"), ",")
			if tag[0] == "-" {
				continue
			}
			name := tag[0]
			if name == "" {
				name = f.Name
			}
			properties[name] = schemaType(f.Type, request, seen)
			requiredField := !strings.Contains(","+f.Tag.Get("json")+",", ",omitempty,") && !strings.Contains(","+f.Tag.Get("json")+",", ",omitzero,")
			if request {
				requiredField = strings.Contains(","+f.Tag.Get("binding")+",", ",required,")
			}
			if requiredField {
				required = append(required, name)
			}
		}
		s := map[string]any{"type": "object", "properties": properties}
		if len(required) > 0 {
			s["required"] = required
		}
		return s
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": schemaType(t.Elem(), request, seen)}
	default:
		return map[string]any{}
	}
}

// Schema uses JSON field tags for encoding and Gin binding tags for requests.
// Pointer fields permit null; omitempty/omitzero only affect response presence.
func Schema(value any, request bool) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return schemaType(reflect.TypeOf(value), request, map[reflect.Type]bool{})
}

// Generate is deterministic: encoding/json sorts all map keys and the caller's
// route order fixes parameter/response alternatives. No environment is consulted.
func Generate(routes []Route) ([]byte, error) {
	paths := map[string]any{}
	ids := map[string]bool{}
	for _, route := range routes {
		if route.Method == "" || route.Path == "" || route.ID == "" || len(route.Responses) == 0 {
			return nil, fmt.Errorf("incomplete HTTP metadata for %s", route.Path)
		}
		if ids[route.ID] {
			return nil, fmt.Errorf("duplicate operation ID %s", route.ID)
		}
		ids[route.ID] = true
		path, _ := paths[route.Path].(map[string]any)
		if path == nil {
			path = map[string]any{}
			paths[route.Path] = path
		}
		method := strings.ToLower(route.Method)
		if path[method] != nil {
			return nil, fmt.Errorf("duplicate route %s %s", route.Method, route.Path)
		}
		op := map[string]any{"operationId": route.ID, "summary": route.Summary}
		if route.Authenticated {
			op["security"] = []any{map[string]any{"bearerAuth": []string{}}}
		} else {
			op["security"] = []any{}
		}
		if route.Request != nil {
			s := Schema(route.Request, true)
			if len(route.RequiredFields) > 0 {
				required, _ := s["required"].([]string)
				props := s["properties"].(map[string]any)
				for _, name := range route.RequiredFields {
					if props[name] == nil {
						return nil, fmt.Errorf("unknown required field %s", name)
					}
					required = append(required, name)
					p := props[name].(map[string]any)
					if alternatives, ok := p["anyOf"].([]any); ok {
						props[name] = alternatives[0]
					}
				}
				s["required"] = required
			}
			op["requestBody"] = map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": s}}}
		}
		if route.Query != nil {
			t := reflect.TypeOf(route.Query)
			parameters := []any{}
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				name := f.Tag.Get("form")
				if name == "" || name == "-" {
					continue
				}
				s := schemaType(f.Type, true, map[reflect.Type]bool{})
				p := map[string]any{"name": name, "in": "query", "required": false, "schema": s}
				for key, value := range route.QueryRules[name] {
					if key == "required" || key == "description" {
						p[key] = value
					} else {
						s[key] = value
					}
				}
				parameters = append(parameters, p)
			}
			op["parameters"] = parameters
		}
		responses := map[string]any{}
		for _, response := range route.Responses {
			status := strconv.Itoa(response.Status)
			if responses[status] != nil {
				return nil, fmt.Errorf("duplicate response %s for %s", status, route.Path)
			}
			description := response.Description
			if description == "" {
				description = http.StatusText(response.Status)
			}
			r := map[string]any{"description": description}
			if len(response.Bodies) > 0 {
				s := Schema(response.Bodies[0], false)
				if len(response.Bodies) > 1 {
					alternatives := []any{}
					for _, body := range response.Bodies {
						alternatives = append(alternatives, Schema(body, false))
					}
					s = map[string]any{"anyOf": alternatives}
				}
				r["content"] = map[string]any{"application/json": map[string]any{"schema": s}}
			}
			responses[status] = r
		}
		if route.Authenticated {
			responses["401"] = map[string]any{"description": "Missing, invalid or expired Bearer JWT", "content": map[string]any{"application/json": map[string]any{"schema": Schema(ErrorBody{}, false)}}}
		}
		op["responses"] = responses
		path[method] = op
	}
	doc := map[string]any{"openapi": "3.1.0", "jsonSchemaDialect": "https://json-schema.org/draft/2020-12/schema", "info": map[string]any{"title": "Fable Logic HTTP API", "version": "1.0.0", "description": "Generated by go run ./cmd/httpdocs from registered code-first operations. Static /chat assets and WebSocket transport are excluded."}, "paths": paths, "components": map[string]any{"securitySchemes": map[string]any{"bearerAuth": map[string]any{"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}}}}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
