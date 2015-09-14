#!/bin/sh
#
# add_sleep.sh job_name sleep_seconds
#
curl -d "{\"name\":\"$1\", \"path\": \"/bin/sleep\", \"args\": [\"$2\"]}" localhost:8080/add
