// Package templates provides the HTML template rendering functionality
package templates

import (
	"html/template"
	"io"

	"github.com/labstack/echo/v4"
)

// TemplateRenderer is a custom html/template renderer for Echo framework
type TemplateRenderer struct {
	templates *template.Template
}

// NewTemplateRenderer creates a new TemplateRenderer
func NewTemplateRenderer(glob string) (*TemplateRenderer, error) {
	tmpl, err := template.ParseGlob(glob)
	if err != nil {
		return nil, err
	}
	return &TemplateRenderer{
		templates: tmpl,
	}, nil
}

// Render renders a template document
func (t *TemplateRenderer) Render(w io.Writer, name string, data interface{}, _ echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}
