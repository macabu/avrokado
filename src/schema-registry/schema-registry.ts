import got, { Response as GotResponse } from 'got';

import { Type, Schema } from 'avsc';

export type TopicsSchemas = { [topicName: string]: Schemas };
export type TypeSchemas = { [schemaId: number]: SchemaObject };

export interface ISchemaRegistryResponse {
  subject: string;
  version: number;
  id: number;
  schema: JSON;
}

export interface SchemaObject {
  version: number;
  schema: Type;
}

export interface Schemas {
  valueSchema: TypeSchemas;
  keySchema: TypeSchemas;
}

export class SchemaRegistry {
  private endpoint: string;
  private topics: ReadonlyArray<string> | string;
  private version: number | 'latest' | 'all';

  public schemas: TopicsSchemas = {};

  constructor(
    endpoint: string,
    topics: ReadonlyArray<string> | string,
    version: number | 'latest' | 'all'
  ) {
    this.endpoint = endpoint.endsWith('/') ? endpoint.slice(0, -1) : endpoint;
    this.topics = typeof topics === 'string' ? [topics] : topics;
    this.version = version;
  }

  public async load() {
    for await (const topic of this.topics) {
      const [valueSchemasArray, keySchemasArray] = await Promise.all([
        this.loadByType(topic, 'value'),
        this.loadByType(topic, 'key'),
      ]);

      if (valueSchemasArray && keySchemasArray) {
        this.schemas[topic] = {
          valueSchema: valueSchemasArray,
          keySchema: keySchemasArray,
        };
      }
    }
  }

  private async loadByType(topic: string, type: 'value' | 'key') {
    let iterVersions = [this.version];
    if (this.version === 'all') {
      const versions = await this.fetch(topic, type);
      iterVersions = (<number[]>versions).sort((a, b) => b - a);
    }

    const typeSchemas: TypeSchemas = {};
    for await (const version of iterVersions) {
      const schema = await this.fetch(topic, type, <'latest' | number>version);
      if (schema.id) {
        typeSchemas[schema.id] = <SchemaObject>{
          version: schema.version,
          schema: Type.forSchema(<Schema>((schema.schema) as unknown), { wrapUnions: true }),
        };
      }
    }
    return typeSchemas;
  }

  private async fetch<T extends ISchemaRegistryResponse & number[]>(
    topic: string,
    type: 'value' | 'key',
    version?: 'latest' | number
  ) {
    let endpoint = `${this.endpoint}/subjects/${topic}-${type}/versions`;
    if (version) {
      endpoint += `/${version}`;
    }

    const response = await got.get(endpoint, { json: true });

    if (response && response.body && response.statusCode === 200) {
      const { body }: GotResponse<T> = response;

      if (!body.schema) {
        return body;
      }

      return {
        ...body,
        schema: JSON.parse(<string>(body.schema as unknown)),
      };
    }

    throw TypeError(`Fetch schema: (${response.statusCode}) ${response.statusMessage}`);
  }
}
