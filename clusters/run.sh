#!/bin/bash

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

for node in {1..5}; do
  (cd "$ROOT_DIR" && go run ./main.go --config "clusters/node${node}/config.yaml") &
done