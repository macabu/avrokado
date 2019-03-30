import got, { Response as GotResponse } from 'got';

export type SchemaRegistryEndpoint = string;
export type TopicName = string;
export type SchemaVersion = 'latest' | number;
export type SchemaFetchType = 'key' | 'value';
export type SchemaFetchResponse<T> = T & Partial<IFetchSchemaFailed>;
export type SchemaFetchVersionsResponse = number[];

export interface ISchemaRegistryResponse {
  subject: string;
  version: number;
  id: number;
  schema: JSON;
}

interface IFetchSchemaFailedAxiosResponse {
  status: number;
  statusText: string;
  method: string;
  path: string;
  data: unknown;
}

interface IFetchSchemaFailed {
  topicName: TopicName;
  schemaVersion: SchemaVersion;
  type: SchemaFetchType;
  message: string;
  axiosResponse: IFetchSchemaFailedAxiosResponse;
}

export const fetchSchema = async (
  schemaRegistryEndpoint: SchemaRegistryEndpoint,
  topicName: TopicName,
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

    return <SchemaFetchResponse<Partial<ISchemaRegistryResponse>>>{
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
  schemaRegistryEndpoint: SchemaRegistryEndpoint,
  topicName: TopicName,
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

    return <SchemaFetchResponse<Partial<unknown>>> {
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
