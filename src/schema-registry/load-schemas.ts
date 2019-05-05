import { Type, Schema } from 'avsc';

import {
  fetchSchema,
  fetchSchemaVersions,
  SchemaVersion,
  SchemaFetchType,
} from './fetch';

export type TopicsSchemas = { [topicName: string]: Schemas };
export type TypeSchemas = { [schemaId: number]: SchemaObject };
export type SchemaVersionsRequested = SchemaVersion | 'all';

export interface SchemaObject {
  version: number;
  schema: Type;
}
export interface Schemas {
  valueSchema: TypeSchemas;
  keySchema: TypeSchemas;
}

export const loadSchemasByType = async (
  schemaRegistryEndpoint: string,
  topicName: string,
  schemaVersions: SchemaVersionsRequested,
  type: SchemaFetchType
) => {
  try {
    const typeSchemas: TypeSchemas = {};

    if (schemaVersions === 'latest') {
      const schema = await fetchSchema(
        schemaRegistryEndpoint,
        topicName,
        schemaVersions,
        type
      );

      if (schema.id) {
        typeSchemas[schema.id] = <SchemaObject>{
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
          typeSchemas[schema.id] = <SchemaObject>{
            version: schema.version,
            schema: Type.forSchema(
              <Schema>((schema.schema) as unknown),
              { wrapUnions: true }
            ),
          };
        }
      }
    }

    return typeSchemas;
  } catch (error) {
    throw error;
  }
};

export const loadSchemas = async (
  schemaRegistryEndpoint: string,
  topics: ReadonlyArray<string> | string,
  schemaVersions: SchemaVersionsRequested
) => {
  try {
    const topicsSchemas: TopicsSchemas = {};

    let saneTopics = topics;
    if (typeof topics === 'string') {
      saneTopics = [topics];
    }

    for await (const topic of saneTopics) {
      const valueSchemasArray =
        await loadSchemasByType(schemaRegistryEndpoint, topic, schemaVersions, 'value');

      const keySchemasArray =
        await loadSchemasByType(schemaRegistryEndpoint, topic, schemaVersions, 'key');

      topicsSchemas[topic] = {
        valueSchema: valueSchemasArray,
        keySchema: keySchemasArray,
      };
    }

    return topicsSchemas;
  } catch (error) {
    throw error;
  }
};
