import { Type, Schema } from 'avsc';

import {
  fetchSchema,
  fetchSchemaVersions,
  SchemaRegistryEndpoint,
  TopicName,
  SchemaVersion,
  SchemaFetchType,
} from '../schema-registry/fetch';

type SchemaVersionsRequested = SchemaVersion | 'all';

export const loadSchemasByType = async (
  schemaRegistryEndpoint: SchemaRegistryEndpoint,
  topicName: TopicName,
  schemaVersions: SchemaVersionsRequested,
  type: SchemaFetchType
) => {
  try {
    const schemaMap = new Map();

    if (schemaVersions === 'latest') {
      const schema = await fetchSchema(
        schemaRegistryEndpoint,
        topicName,
        schemaVersions,
        type
      );

      schemaMap.set(schema.id, {
        version: schema.version,
        schema: Type.forSchema(<Schema>((schema.schema) as unknown)),
      });
    } else {
      let versions;

      if (typeof schemaVersions === 'number') {
        versions = [schemaVersions];
      }

      if (schemaVersions === 'all') {
        versions = await fetchSchemaVersions(schemaRegistryEndpoint, topicName, type);
      }

      const orderedVersions = (<number[]>versions).sort((a, b) => b - a);

      for await (const schemaVersion of orderedVersions) {
        const schema = await fetchSchema(
          schemaRegistryEndpoint,
          topicName,
          schemaVersion,
          type
        );

        schemaMap.set(schema.id, {
          version: schema.version,
          schema: Type.forSchema(<Schema>((schema.schema) as unknown)),
        });
      }
    }

    return schemaMap;
  } catch (error) {
    throw error;
  }
};

export const loadSchemas = async (
  schemaRegistryEndpoint: SchemaRegistryEndpoint,
  topicName: TopicName,
  schemaVersions: SchemaVersionsRequested
) => {
  try {
    const topicSchemaMap = new Map();

    const valueSchemasMap =
      await loadSchemasByType(schemaRegistryEndpoint, topicName, schemaVersions, 'value');

    const keySchemasMap =
      await loadSchemasByType(schemaRegistryEndpoint, topicName, schemaVersions, 'key');

    topicSchemaMap.set('value', valueSchemasMap);
    topicSchemaMap.set('key', keySchemasMap);

    return topicSchemaMap;
  } catch (error) {
    throw error;
  }
};
