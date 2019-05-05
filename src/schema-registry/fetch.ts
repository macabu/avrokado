import got, { Response as GotResponse } from 'got';

export type SchemaVersion = 'latest' | number;
export type SchemaFetchType = 'key' | 'value';
export type SchemaFetchResponse<T> = T | Partial<T>;
export type SchemaFetchVersionsResponse = number[];

export interface ISchemaRegistryResponse {
  subject: string;
  version: number;
  id: number;
  schema: JSON;
}

export const fetchSchema = async (
  schemaRegistryEndpoint: string,
  topicName: string,
  schemaVersion: SchemaVersion,
  type: SchemaFetchType
) => {
  try {
    let saneEndpoint = '';

    saneEndpoint += `${schemaRegistryEndpoint}${(schemaRegistryEndpoint.endsWith('/') ? '' : '/')}`;
    saneEndpoint += 'subjects/';
    saneEndpoint += `${topicName}-${type}/`;
    saneEndpoint += 'versions/';
    saneEndpoint += `${schemaVersion}`;

    const response: GotResponse<ISchemaRegistryResponse> =
      await got.get(saneEndpoint, { json: true });

    if (response && response.body && response.statusCode === 200) {
      const { body }: GotResponse<ISchemaRegistryResponse> = response;

      return <SchemaFetchResponse<ISchemaRegistryResponse>>{
        ...body,
        schema: JSON.parse(<string>(body.schema as unknown)),
      };
    }

    return <SchemaFetchResponse<ISchemaRegistryResponse>>{
      topicName,
      schemaVersion,
      type,
      message: 'Schema fetch failed abnormously!',
      httpResponse: {
        status: response.statusCode,
        statusText: response.statusMessage,
        method: response.method,
        path: response.url,
        body: response.body,
      },
    };
  } catch (error) {
    throw error;
  }
};

export const fetchSchemaVersions = async (
  schemaRegistryEndpoint: string,
  topicName: string,
  type: SchemaFetchType
) => {
  try {
    let saneEndpoint = '';

    saneEndpoint += `${schemaRegistryEndpoint}${(schemaRegistryEndpoint.endsWith('/') ? '' : '/')}`;
    saneEndpoint += 'subjects/';
    saneEndpoint += `${topicName}-${type}/`;
    saneEndpoint += 'versions';

    const response = await got.get(saneEndpoint, { json: true });

    if (response && response.body && response.statusCode === 200) {
      const { body }: GotResponse<SchemaFetchResponse<SchemaFetchVersionsResponse>> = response;

      return body;
    }

    return <SchemaFetchResponse<ISchemaRegistryResponse>>{
      topicName,
      type,
      message: 'Schema versions fetch failed abnormously!',
      httpResponse: {
        status: response.statusCode,
        statusText: response.statusMessage,
        method: response.method,
        path: response.url,
        body: response.body,
      },
    };
  } catch (error) {
    throw error;
  }
};
