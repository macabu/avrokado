import { Type, Schema } from 'avsc';

import {
  fetchSchema,
  fetchSchemaVersions,
  SchemaRegistryEndpoint,
  TopicName,
  SchemaVersion,
  SchemaFetchType,
} from '../schema-registry/fetch';

export interface SchemaObject {
  version: number;
  schema: Type;
}

type SchemaVersionsRequested = SchemaVersion | 'all';

export const loadSchemasByType = async (
  schemaRegistryEndpoint: SchemaRegistryEndpoint,
  topicName: TopicName,
  schemaVersions: SchemaVersionsRequested,
  type: SchemaFetchType
) => {
  try {
    const schemaArray: SchemaObject[] = [];

    if (schemaVersions === 'latest') {
      const schema = await fetchSchema(
        schemaRegistryEndpoint,
        topicName,
        schemaVersions,
        type
      );

      if (schema.id) {
        schemaArray[schema.id] = <SchemaObject>{
          version: schema.version,
          schema: Type.forSchema(
            <Schema>((schema.schema) as unknown),
            { wrapUnions: true }
          ),
        };
      }
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

        if (schema.id) {
          schemaArray[schema.id] = <SchemaObject>{
            version: schema.version,
            schema: Type.forSchema(
              <Schema>((schema.schema) as unknown),
              { wrapUnions: true }
            ),
          };
        }
      }
    }

    return schemaArray;
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
    const topicSchemaMap = new Map<string, SchemaObject[]>();

    const valueSchemasArray =
      await loadSchemasByType(schemaRegistryEndpoint, topicName, schemaVersions, 'value');

    const keySchemasArray =
      await loadSchemasByType(schemaRegistryEndpoint, topicName, schemaVersions, 'key');

    topicSchemaMap.set('value', valueSchemasArray);
    topicSchemaMap.set('key', keySchemasArray);

    return topicSchemaMap;
  } catch (error) {
    throw error;
  }
};
