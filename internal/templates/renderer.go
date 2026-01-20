// Package templates provides the HTML template rendering functionality
package templates

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"os"

	"github.com/labstack/echo/v4"
)

// TemplateRenderer is a custom html/template renderer for Echo framework
type TemplateRenderer struct {
	templates *template.Template
	manifest  map[string]manifestEntry
}

type manifestEntry struct {
	File string `json:"file"`
	Src  string `json:"src"`
}

// NewTemplateRenderer creates a new TemplateRenderer
func NewTemplateRenderer(glob string, manifestPath string) (*TemplateRenderer, error) {
	t := &TemplateRenderer{
		manifest: make(map[string]manifestEntry),
	}

	// Load manifest if it exists
	if manifestPath != "" {
		if err := t.loadManifest(manifestPath); err != nil {
			// Don't fail hard if manifest is missing (e.g. in dev), just log or ignore
			fmt.Printf("Warning: failed to load manifest: %v\n", err)
		}
	}

	// Define FuncMap
	funcMap := template.FuncMap{
		"asset": t.assetPath,
	}

	tmpl, err := template.New("").Funcs(funcMap).ParseGlob(glob)
	if err != nil {
		return nil, err
	}
	t.templates = tmpl

	return t, nil
}

func (t *TemplateRenderer) loadManifest(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &t.manifest)
}

func (t *TemplateRenderer) assetPath(key string) string {
	if entry, ok := t.manifest[key]; ok {
		return "/static/" + entry.File
	}
	// Fallback or dev mode handling could go here
	return "/static/" + key
}

// Render renders a template document
func (t *TemplateRenderer) Render(w io.Writer, name string, data interface{}, _ echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}
