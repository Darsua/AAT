#!/usr/bin/env bash
# Render every docs/diagrams/*.mmd to SVG and PNG in docs/diagrams/rendered/, and the report figure to
# docs/report/figures/arsitektur.pdf. Needs Node.js and a Chromium-based browser (set BROWSER_PATH).
set -euo pipefail
cd "$(dirname "$0")"
BROWSER_PATH="${BROWSER_PATH:-/usr/bin/brave}"
export PUPPETEER_SKIP_DOWNLOAD=1
PCFG="$(mktemp)"
printf '{"executablePath":"%s","args":["--no-sandbox","--disable-gpu"]}' "$BROWSER_PATH" > "$PCFG"
MMDC=(npx -y @mermaid-js/mermaid-cli@11.4.2 -p "$PCFG")
mkdir -p rendered ../report/figures

for f in *.mmd; do
  name="${f%.mmd}"
  [ "$name" = "architecture.report" ] && continue
  for ext in svg png; do
    "${MMDC[@]}" -c mermaid-config.json -i "$f" -o "rendered/$name.$ext" -b white -s 2
  done
done

"${MMDC[@]}" -c mermaid-config.report.json -i architecture.report.mmd \
  -o ../report/figures/arsitektur.pdf -e pdf --pdfFit -w 1000 -b white
rm -f "$PCFG"
python3 check_sync.py
