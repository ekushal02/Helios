// Command ampplot runs compaction.MeasureAmplification at three
// maxFilesPerLevel configurations, prints the results as a
// copy-pasteable Markdown table, and renders them as an SVG chart --
// the "plot them" this task asks for.
//
// No third-party charting dependency is used or added -- this project's
// go.mod has never had one, and three configurations' worth of bar
// charts is well within what a hand-written SVG can do plainly. Usage:
//
//	go run ./cmd/ampplot [output-path]
//
// output-path defaults to amplification.svg at the repository root,
// alongside fsync-policy.md -- this project's other measured, written-up
// artifact that isn't part of DESIGN.md itself.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/ekushal02/helios/internal/storage/compaction"
)

func main() {
	configs := []int{2, 4, 8}
	results := make([]*compaction.AmplificationResult, len(configs))
	for i, c := range configs {
		r, err := compaction.MeasureAmplification(c)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ampplot: MeasureAmplification(%d): %v\n", c, err)
			os.Exit(1)
		}
		results[i] = r
	}

	fmt.Println("| maxFilesPerLevel | write amplification | peak space amplification | final space amplification |")
	fmt.Println("|---|---|---|---|")
	for _, r := range results {
		fmt.Printf("| %d | %.3f | %.3f | %.3f |\n", r.MaxFilesPerLevel, r.WriteAmplification, r.PeakSpaceAmplification, r.SpaceAmplification)
	}

	outPath := "amplification.svg"
	if len(os.Args) > 1 {
		outPath = os.Args[1]
	}
	if err := os.WriteFile(outPath, []byte(renderSVG(results)), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "ampplot: write %s: %v\n", outPath, err)
		os.Exit(1)
	}
	fmt.Printf("\nchart written to %s\n", outPath)
}

// renderSVG draws two stacked bar-chart panels sharing the same
// maxFilesPerLevel x-axis: write amplification on top, peak space
// amplification on the bottom, with a dashed reference line marking
// final (fully-converged) space amplification -- constant across all
// three configurations, by the design argued in MeasureAmplification's
// own doc, so a single reference line is the correct way to show it
// rather than a third set of bars that would just repeat one value
// three times.
func renderSVG(results []*compaction.AmplificationResult) string {
	const (
		width      = 720
		panelH     = 220
		panelGap   = 60
		leftMargin = 70
		rightPad   = 40
		topPad     = 30
		barW       = 90
		barGap     = 60
	)
	height := topPad + panelH + panelGap + panelH + 60

	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %d %d" xmlns="http://www.w3.org/2000/svg" font-family="monospace" font-size="13">`+"\n", width, height)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="#ffffff"/>`+"\n", width, height)

	// -- Panel 1: write amplification --
	maxWA := 0.0
	for _, r := range results {
		if r.WriteAmplification > maxWA {
			maxWA = r.WriteAmplification
		}
	}
	scaleWA := maxWA * 1.15
	panel1Y := topPad
	drawPanel(&b, "Write amplification (physical bytes / logical bytes)", results, panel1Y, panelH, leftMargin, rightPad, barW, barGap,
		func(r *compaction.AmplificationResult) float64 { return r.WriteAmplification }, scaleWA, "#3b6fb5", nil)

	// -- Panel 2: peak space amplification, with a dashed final-SA reference line --
	maxSA := 0.0
	for _, r := range results {
		if r.PeakSpaceAmplification > maxSA {
			maxSA = r.PeakSpaceAmplification
		}
	}
	scaleSA := maxSA * 1.15
	panel2Y := topPad + panelH + panelGap
	finalSA := results[0].SpaceAmplification // identical across configs by design; see doc above
	drawPanel(&b, "Peak space amplification (on-disk bytes / live logical bytes)", results, panel2Y, panelH, leftMargin, rightPad, barW, barGap,
		func(r *compaction.AmplificationResult) float64 { return r.PeakSpaceAmplification }, scaleSA, "#b5673b", &finalSA)

	b.WriteString(`</svg>`)
	return b.String()
}

// drawPanel renders one bar-chart panel: an axis, one bar per result
// (value given by valueFn), its numeric label above the bar, and an
// x-axis tick per maxFilesPerLevel. If refLine is non-nil, a dashed
// horizontal line is drawn at that value with its own label -- used for
// the constant final-space-amplification reference in panel 2.
func drawPanel(b *strings.Builder, title string, results []*compaction.AmplificationResult, y, panelH, leftMargin, rightPad, barW, barGap int,
	valueFn func(*compaction.AmplificationResult) float64, scale float64, color string, refLine *float64) {

	plotH := panelH - 40 // leave room for the title and x-axis labels
	baseline := y + 20 + plotH

	fmt.Fprintf(b, `<text x="%d" y="%d" font-weight="bold">%s</text>`+"\n", leftMargin, y, title)
	// axis
	fmt.Fprintf(b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#333" stroke-width="1"/>`+"\n",
		leftMargin, y+20, leftMargin, baseline)
	fmt.Fprintf(b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#333" stroke-width="1"/>`+"\n",
		leftMargin, baseline, leftMargin+len(results)*(barW+barGap), baseline)

	if refLine != nil {
		refY := baseline - int(float64(plotH)*(*refLine/scale))
		fmt.Fprintf(b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#555" stroke-width="1.5" stroke-dasharray="6,4"/>`+"\n",
			leftMargin, refY, leftMargin+len(results)*(barW+barGap), refY)
		fmt.Fprintf(b, `<text x="%d" y="%d" fill="#555">final SA = %.3f (all configs)</text>`+"\n",
			leftMargin+len(results)*(barW+barGap)-260, refY-6, *refLine)
	}

	for i, r := range results {
		v := valueFn(r)
		barH := int(float64(plotH) * (v / scale))
		x := leftMargin + barGap/2 + i*(barW+barGap)
		barY := baseline - barH
		fmt.Fprintf(b, `<rect x="%d" y="%d" width="%d" height="%d" fill="%s"/>`+"\n", x, barY, barW, barH, color)
		fmt.Fprintf(b, `<text x="%d" y="%d" text-anchor="middle">%.3f</text>`+"\n", x+barW/2, barY-6, v)
		fmt.Fprintf(b, `<text x="%d" y="%d" text-anchor="middle">maxFilesPerLevel=%d</text>`+"\n", x+barW/2, baseline+20, r.MaxFilesPerLevel)
	}
}
