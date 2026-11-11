#!/bin/sh
set -eu

root="$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)"
image="${MEDIA_TOOLS_CONTAINER_IMAGE:-mirrorcore-media-tools:local}"
fixture_dir="$root/assets/localdemo"
mkdir -p "$fixture_dir"
say -v Samantha -r 165 -o "$fixture_dir/speech.aiff" \
  'Kafka passes parse jobs from the API to workers. A worker checks stored progress before doing the work again.'
podman run --rm --volume "$fixture_dir:/media" "$image" ffmpeg -hide_banner -loglevel error \
  -f lavfi -i color=c=navy:s=640x360:r=24:d=12 -i /media/speech.aiff \
  -c:v libx264 -preset veryfast -pix_fmt yuv420p -c:a aac -b:a 96k -shortest \
  -movflags +faststart /media/voice-demo.mp4
rm -f "$fixture_dir/speech.aiff"
