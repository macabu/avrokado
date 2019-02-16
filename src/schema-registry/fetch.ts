import axios, { AxiosResponse } from 'axios';

export type SchemaRegistryEndpoint = string;
export type TopicName = string | void;
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

    const response = await axios.get(saneEndpoint);

    if (response && response.data && response.status === 200) {
      const { data }: AxiosResponse<ISchemaRegistryResponse> = response;

      return <SchemaFetchResponse<ISchemaRegistryResponse>>{
        ...data,
        schema: JSON.parse(<string>(data.schema as unknown)),
      };
    }

    return <SchemaFetchResponse<Partial<ISchemaRegistryResponse>>>{
      topicName,
      schemaVersion,
      type,
      message: 'Schema fetch failed abnormously!',
      httpResponse: {
        status: response.status,
        statusText: response.statusText,
        method: response.request.method,
        path: response.request.path,
        data: response.data,
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

    const response = await axios.get(saneEndpoint);

    if (response && response.data && response.status === 200) {
      const { data }: AxiosResponse<SchemaFetchResponse<SchemaFetchVersionsResponse>> = response;

      return data;
    }

    return <SchemaFetchResponse<Partial<unknown>>> {
      topicName,
      type,
      message: 'Schema versions fetch failed abnormously!',
      httpResponse: {
        status: response.status,
        statusText: response.statusText,
        method: response.request.method,
        path: response.request.path,
        data: response.data,
      },
    };
  } catch (error) {
    throw error;
  }
};
