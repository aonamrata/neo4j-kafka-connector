/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.connectors.kafka.data

import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.neo4j.cdc.client.model.CaptureMode
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.cdc.client.model.State
import org.neo4j.connectors.kafka.events.NodePayload
import org.neo4j.connectors.kafka.events.OperationType
import org.neo4j.connectors.kafka.events.RelationshipPayload
import org.neo4j.connectors.kafka.events.StreamsConstraintType
import org.neo4j.connectors.kafka.events.StreamsTransactionEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object StreamsTransactionEventExtensions {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  private const val SOURCE_ID_FIELD_NAME: String = "v4ElementId"

  fun StreamsTransactionEvent.toChangeEvent(): ChangeEvent {
    val cdcOperation =
        when (this.meta.operation) {
          OperationType.created -> EntityOperation.CREATE
          OperationType.updated -> EntityOperation.UPDATE
          OperationType.deleted -> EntityOperation.DELETE
        }
    val cdcMetadata =
        Metadata(
            this.meta.username,
            this.meta.username,
            "unknown",
            "unknown",
            CaptureMode.OFF,
            "unknown",
            "unknown",
            "unknown",
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(this.meta.timestamp), ZoneOffset.UTC),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(this.meta.timestamp), ZoneOffset.UTC),
            emptyMap(),
            emptyMap())
    logger.trace("In StreamsTransactionEvent.toChangeEvent for cdcOperation: {}", cdcOperation)

    val cdcEvent =
        when (val payload = this.payload) {
          is NodePayload -> {
            val before = payload.before?.let { NodeState(it.labels, it.properties) }
            before?.properties?.set(SOURCE_ID_FIELD_NAME, payload.id)
            val after = payload.after?.let { NodeState(it.labels, it.properties) }
            after?.properties?.set(SOURCE_ID_FIELD_NAME, payload.id)

            val referenceState = extractState(this, before, after)
            logger.trace("In StreamsTransactionEvent referenceState: {}", referenceState)

            val keys =
                this.schema.constraints
                    .filter { c -> c.type == StreamsConstraintType.UNIQUE }
                    .map { c ->
                      c.label!! to
                          c.properties
                              .let {
                                if (cdcOperation == EntityOperation.DELETE)
                                    it.plus(SOURCE_ID_FIELD_NAME)
                                else it
                              }
                              .associateWith { referenceState.properties[it] }
                              // Does not have values associated in the schema
                              .filterValues { it != null }
                    }
                    .filter { it.second.isNotEmpty() }
                    .groupBy { it.first }
                    .mapValues { it.value.map { p -> p.second } }

            logger.trace(
                "In StreamsTransactionEvent for operation: {} Node keys: {}", cdcOperation, keys)
            NodeEvent(payload.id, cdcOperation, referenceState.labels, keys, before, after)
          }
          // Relationships.
          is RelationshipPayload -> {
            val before = payload.before?.let { RelationshipState(it.properties) }
            before?.properties?.set(SOURCE_ID_FIELD_NAME, payload.id)
            val after = payload.after?.let { RelationshipState(it.properties) }
            after?.properties?.set(SOURCE_ID_FIELD_NAME, payload.id)
            var relKeys: List<Map<String, Any>> = emptyList()
            if (cdcOperation == EntityOperation.DELETE) {
              relKeys = relKeys.plus(mapOf(SOURCE_ID_FIELD_NAME to payload.id))
            }
            logger.trace(
                "In StreamsTransactionEvent for operation: {} Rel keys: {}",
                cdcOperation,
                relKeys,
            )
            // This is to avoid creating a dummy node Orchard with id same as Track id.
            // @todo make this as config param.
            val skipRelNodeLabels =
                arrayOf(
                    "Orchard",
                    "MusicGraph",
                    "AccountParticipant",
                    "Chartmetric",
                    "ChartmetricAmazon",
                    "ChartmetricAppleMusic",
                    "ChartmetricDeezer",
                    "ChartmetricItunes",
                    "ChartmetricLinemusic",
                    "ChartmetricRecochoku",
                    "ChartmetricSoundCloud",
                    "ChartmetricSpotify",
                    "ChartmetricTikTok",
                    "CountryFilter",
                    "FolkUke",
                    "LabelSoundRecordingParticipation",
                    "Official",
                    "Other",
                    "PublicVideo",
                    "Spotify",
                    "YouTube",
                    "YoutubeAsset",
                )

            RelationshipEvent(
                payload.id,
                payload.label,
                Node(
                    payload.start.id,
                    payload.start.labels ?: emptyList(),
                    buildMap {
                      val label = payload.start.labels?.firstOrNull { it !in skipRelNodeLabels }
                      if (label != null) {
                        this[label] = listOf(payload.start.ids)
                      }
                    }),
                Node(
                    payload.end.id,
                    payload.end.labels ?: emptyList(),
                    buildMap {
                      val label = payload.end.labels?.firstOrNull { it !in skipRelNodeLabels }
                      if (label != null) {
                        this[label] = listOf(payload.end.ids)
                      }
                    }),
                relKeys,
                cdcOperation,
                before,
                after)
          }
          else ->
              throw IllegalArgumentException("unexpected payload type ${payload.javaClass.name}")
        }

    return ChangeEvent(
        ChangeIdentifier("${this.meta.txId}:${this.meta.txEventId}"),
        this.meta.txId,
        this.meta.txEventId,
        cdcMetadata,
        cdcEvent)
  }

  private fun <T : State> extractState(event: StreamsTransactionEvent, before: T?, after: T?): T {
    val (name, referenceState) =
        when (event.meta.operation) {
          OperationType.created -> ("after" to after)
          OperationType.updated -> ("before" to before)
          OperationType.deleted -> ("before" to before)
        }
    require(referenceState != null) {
      "$name state should not be null for ${event.meta.operation} events"
    }
    return referenceState
  }
}
