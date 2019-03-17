import {
  loadSchemasForTopic,
  cleanupSchemas,
} from '../../utils/create-schema';
import {
  SCHEMA_REGISTRY_URL,
  TOPIC_NAME
} from '../../utils/constant';
import { loadSchemasByType, loadSchemas } from '../../../src/schema-registry/load-schemas';

describe('E2E Test : src/schema-registry/load-schema.ts', () => {
  beforeAll(async () => {
    await cleanupSchemas();

    const result = await loadSchemasForTopic();
    if (!result) {
      throw new Error('Could not upload schemas to schema registry. Exiting...');
    }
  });

  describe('loadSchemasByType', () => {
    test('Loading the latest value schema version', async () => {
      expect.assertions(4);

      const response = await loadSchemasByType(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest',
        'value'
      );

      const schema = response[1].schema;

      expect(typeof response).toBe('object');
      expect(response[1]).toBeTruthy();
      expect(response[1]).toHaveProperty('version');
      expect(schema).toHaveProperty('fields');
    });

    test('Loading a single value schema version which does not exist', async () => {
      expect.assertions(1);

      await expect(
        loadSchemasByType(
          SCHEMA_REGISTRY_URL,
          TOPIC_NAME,
          0,
          'value'
        )
      ).rejects.toThrowError();
    });
  });

  describe('loadSchemas', () => {
    test('Loading the latest value and key schema version', async () => {
      expect.assertions(6);

      const response = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      const valueSchemas = response[TOPIC_NAME].valueSchema;
      const keySchemas = response[TOPIC_NAME].keySchema;

      expect(typeof response).toBe('object');
      expect(typeof valueSchemas).toBe('object');
      expect(typeof keySchemas).toBe('object');
      expect(keySchemas[2]).toBeTruthy();
      expect(keySchemas[2]).toHaveProperty('version');
      expect(keySchemas[2]).toHaveProperty('schema');
    });

    test('When a schema doesnt exist', async () => {
      expect.assertions(1);

      await expect(
        loadSchemas(SCHEMA_REGISTRY_URL, ['fail-topic'], 'latest')
      ).rejects.toThrowError();
    });
  });

  afterAll(() => cleanupSchemas());
});
