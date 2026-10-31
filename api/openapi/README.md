# Logic HTTP OpenAPI

`http.json` is generated OpenAPI **3.1.0**, using the JSON Schema 2020-12 dialect. Do not edit it directly.

From the repository root:

```sh
go run ./cmd/httpdocs
sh scripts/check-httpdocs.sh
```

The generator is fixed, repository-versioned Go code (`internal/httpdocs` and `cmd/httpdocs`) and uses the module's Go version; it downloads no separate generator. Route method, path, handler, authentication, request/response types, and status annotations are co-located in `internal/https_server/route_catalog.go`. Both HTTP constructors mount those operations. `NewEngine` additionally retains its legacy WebSocket route; `NewLogicEngine` mounts the supplied sync handler without `/wss`.

The generator reflects JSON/form and Gin binding tags. Explicit controller checks (such as the required non-null AI allow list), sync query bounds, and error statuses live beside registration. Response fields without `omitempty`/`omitzero` are required in the schema; request presence follows binding tags and explicit controller annotations. Pointer values allow null unless an explicit controller check rejects it. Legacy history is a bare array and legacy `error` objects, raw Go-error JSON, and empty 204 responses retain their separate shapes.

WebSocket frames and static `/chat` assets are deliberately excluded from REST documentation. The drift check generates into a temporary directory, compares the committed artifact, and runs route/auth/response contract tests. Tests cover every mounted REST method/path and missing-JWT denial, plus real history success/empty/error responses, public validation errors, and sync middleware wiring. They do not exhaust every database-backed business branch or infer authorization rules from DTOs; those remain controller/service tests and registration annotations.
