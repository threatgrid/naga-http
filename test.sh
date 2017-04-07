#!/bin/bash

set -e

server=http://localhost:3000
scratch=target/test

rules_in=../naga/data/family.lg
data_in=../naga/data/in.json

function log() {
   echo $1
   echo ""
}

log "Clearing programs."
curl -s -X DELETE $server/rules
log

uuid=`curl -s -f -H "Content-Type: text/plain" -d @$rules_in $server/rules || echo FAIL`

if [ "$uuid" == "FAIL" ]; then
    echo "Program installation failed."
    exit -1
fi

log "Program installed with UUID = $uuid"

log "Installed program is:"
curl -s $server/rules/$uuid
log

log "Evaluation Result:"
curl -s -H "Content-Type: application/json" -d @$data_in $server/rules/$uuid/eval | jq .
