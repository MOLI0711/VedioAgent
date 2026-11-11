#!/bin/sh
set -eu

tool="$1"
shift
temp_dir="${MEDIA_TEMP_DIR:-/tmp/mirrorcore-media}"
image="${MEDIA_TOOLS_CONTAINER_IMAGE:-mirrorcore-media-tools:local}"
mkdir -p "$temp_dir"
case "$tool" in
  ffmpeg|ffprobe) ;;
  *) echo "unsupported media tool: $tool" >&2; exit 2 ;;
esac
exec podman run --rm --volume "$temp_dir:$temp_dir" "$image" "$tool" "$@"
