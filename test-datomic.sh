#!/bin/bash

set -e

server=http://localhost:3030
scratch=target/test

rules_in=../naga/data/family.lg
data_in=../naga/data/in.json
schema_in=../naga/data/in.schema

function log() {
   echo $1
   echo ""
}

log "Clearing programs."
curl -s -X DELETE ${server}/rules
log

log "Loading data"
curl -s -X POST -H "Content-Type: application/json" -d @${data_in} ${server}/store/data
log

log "Retrieving data"
curl -s -H "Accept: application/json" ${server}/store/data | jq .
log

log "Setting up database."
curl -s -X POST -H "Content-Type: application/json" -d '{"type": "datomic", "uri": "datomic:sql://naga?jdbc:postgresql://localhost:5432/datomic?user=datomic&password=datomic"}' ${server}/store

log "Loading schema"
curl -s -X POST -H "Content-Type: text/plain" --data-binary @${schema_in} ${server}/store/schema
log

log "Loading data"
curl -s -X POST -H "Content-Type: application/json" -d @${data_in} ${server}/store/data
log

log "Retrieving data"
curl -s -H "Accept: application/json" ${server}/store/data | jq .
log


uuid=`curl -s -X POST -f -H "Content-Type: text/plain" -d @$rules_in $server/rules || echo FAIL`

if [ "$uuid" == "FAIL" ]; then
    echo "Program installation failed."
    exit -1
fi

log "Program installed with UUID = $uuid"

log "Installed program is:"
curl -s $server/rules/$uuid
log

log "Evaluation Result:"
curl -s -X POST $server/rules/$uuid/eval | jq .

