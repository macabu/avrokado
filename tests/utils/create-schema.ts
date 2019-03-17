import axios from 'axios';

import { SCHEMA_REGISTRY_URL, TOPIC_NAME } from './constant';
import { keySchema } from './schemas/mocktopic-key';
import { valueSchema } from './schemas/mocktopic-value';

export const loadSchemasForTopic = async () => {
  const resultValue =
    await uploadSchema(SCHEMA_REGISTRY_URL, TOPIC_NAME, JSON.stringify(valueSchema), 'value');
  const resultKey =
    await uploadSchema(SCHEMA_REGISTRY_URL, TOPIC_NAME, JSON.stringify(keySchema), 'key');

  return (resultValue && resultKey);
};

export const cleanupSchemas = async () => {
  const resultValue = await deleteSchema(TOPIC_NAME, 'value');
  const resultKey = await deleteSchema(TOPIC_NAME, 'key');

  return (resultValue && resultKey);
};

export const uploadSchema = async (
  schemaRegistry: string,
  topic: string,
  schema: string,
  type: 'key' | 'value'
) => {
  try {
    await axios({
      url: `${schemaRegistry}/subjects/${topic}-${type}/versions`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/vnd.schemaregistry.v1+json',
      },
      data: {
        schema,
      },
    });

    return true;
  } catch (err) {
    return false;
  }
};

const deleteSchema = async (
  topic: string,
  type: 'key' | 'value'
) => {
  try {
    await axios({
      url: `${SCHEMA_REGISTRY_URL}/subjects/${topic}-${type}`,
      method: 'DELETE',
    });

    return true;
  } catch (err) {
    return false;
  }
};
