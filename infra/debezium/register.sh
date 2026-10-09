#!/usr/bin/env bash
# Registers one Debezium outbox connector per service database (idempotent: PUT replaces the config).
#
#   docker compose -f infra/docker-compose.yml --profile cdc up -d --wait
#   infra/debezium/register.sh
#   OUTBOX_MODE=cdc docker compose -f infra/docker-compose.yml --profile services up -d   # switch relays off
#
# Order matters: the connector creates its replication slot first, so every row written after that is
# captured; the polling relays keep running until the switch and publish anything older. Rows written in
# between go out twice — harmless, every consumer deduplicates on eventId.
set -euo pipefail

CONNECT=${CONNECT_URL:-http://localhost:8093}
DATABASES=${DATABASES:-"inventory booking payment ticketing profile"}
# Dev only: the compose superuser. In a real deployment use a role with REPLICATION and ownership of the
# outbox table (publication.autocreate.mode=filtered creates the publication on that table only).
DB_USER=${CDC_DB_USER:-postgres}
DB_PASSWORD=${CDC_DB_PASSWORD:-postgres}

for db in $DATABASES; do
  curl -sf -X PUT "$CONNECT/connectors/outbox-$db/config" -H 'Content-Type: application/json' -d @- >/dev/null <<JSON
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "plugin.name": "pgoutput",
  "database.hostname": "postgres",
  "database.port": "5432",
  "database.user": "$DB_USER",
  "database.password": "$DB_PASSWORD",
  "database.dbname": "$db",
  "topic.prefix": "cdc.$db",
  "slot.name": "train_outbox_$db",
  "publication.name": "train_outbox_$db",
  "publication.autocreate.mode": "filtered",
  "table.include.list": "public.outbox",

  "snapshot.mode": "no_data",
  "skipped.operations": "u,d,t",
  "tombstones.on.delete": "false",

  "extended.headers.enabled": "false",
  "heartbeat.interval.ms": "10000",
  "topic.creation.default.replication.factor": "1",
  "topic.creation.default.partitions": "1",

  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.predicate": "isOutbox",
  "transforms.outbox.table.field.event.id": "id",
  "transforms.outbox.table.field.event.key": "message_key",
  "transforms.outbox.table.field.event.payload": "payload",
  "transforms.outbox.route.by.field": "topic",
  "transforms.outbox.route.topic.replacement": "\${routedByValue}",
  "transforms.outbox.table.fields.additional.placement": "id:header:eventId,topic:header:eventType,traceparent:header:traceparent",
  "transforms.outbox.table.expand.json.payload": "false",
  "predicates": "isOutbox",
  "predicates.isOutbox.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
  "predicates.isOutbox.pattern": "cdc\\\\.$db\\\\.public\\\\.outbox",

  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter"
}
JSON
  echo "outbox-$db: $(curl -sf "$CONNECT/connectors/outbox-$db/status" | python3 -c 'import json,sys; s=json.load(sys.stdin); print(s["connector"]["state"], [t["state"] for t in s["tasks"]])')"
done
