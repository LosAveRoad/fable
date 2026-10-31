// Command httpdocs writes the generated Logic HTTP OpenAPI 3.1 document.
package main

import (
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"mychat/internal/httpdocs"
	server "mychat/internal/https_server"
	"os"
)

func main() {
	output := flag.String("output", "api/openapi/http.json", "output path; - writes stdout")
	flag.Parse()
	b, err := httpdocs.Generate(server.LogicRoutes(func(*gin.Context) {}))
	if err == nil {
		if *output == "-" {
			_, err = os.Stdout.Write(b)
		} else {
			err = os.WriteFile(*output, b, 0644)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
