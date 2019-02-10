import axios, { AxiosResponse } from 'axios';

type SchemaRegistryEndpoint = string;
type TopicName = string | void;
type LatestSchemaVersion = 'latest' | boolean | number;
type SchemaFetchType = 'key' | 'value';
export type SchemaFetchResponse = Partial<ISchemaRegistryResponse> | Partial<IFetchSchemaFailed>;

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
  latest: LatestSchemaVersion;
  type: SchemaFetchType;
  message: string;
  axiosResponse: IFetchSchemaFailedAxiosResponse;
}

export const fetchSchema = async (
  schemaRegistryEndpoint: SchemaRegistryEndpoint,
  topicName: TopicName,
  latest: LatestSchemaVersion,
  type: SchemaFetchType
) => {
  try {
    let saneEndpoint = '';

    saneEndpoint += `${schemaRegistryEndpoint}${(schemaRegistryEndpoint.endsWith('/') ? '' : '/')}`;
    saneEndpoint += 'subjects/';
    saneEndpoint += `${topicName}-${type}/`;
    saneEndpoint += 'versions/';
    saneEndpoint +=
      `${(typeof latest === 'boolean' || typeof latest === 'string' ? 'latest' : latest)}`;

    const response = await axios.get(saneEndpoint);

    if (response && response.data && response.status === 200) {
      const { data }: AxiosResponse<ISchemaRegistryResponse> = response;

      return <SchemaFetchResponse>{
        ...data,
        schema: JSON.parse(<string>(data.schema as unknown)),
      };
    }

    return <SchemaFetchResponse>{
      topicName,
      latest,
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
