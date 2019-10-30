import {
  loadSchemasForTopic,
  cleanupSchemas,
} from '../../utils/create-schema';
import {
  SCHEMA_REGISTRY_URL,
  TOPIC_NAME
} from '../../utils/constant';
import { SchemaRegistry } from '../../../src/schema-registry';

describe('E2E Test : src/schema-registry/load-schema.ts', () => {
  beforeAll(async () => {
    await cleanupSchemas();

    const result = await loadSchemasForTopic();
    if (!result) {
      throw new Error('Could not upload schemas to schema registry. Exiting...');
    }
  });

  describe('loadSchemas', () => {
    test('Loading the latest value and key schema version', async () => {
      expect.assertions(6);

      const sr = new SchemaRegistry(SCHEMA_REGISTRY_URL, TOPIC_NAME, 'latest');

      await sr.load();

      const valueSchemas = sr.schemas[TOPIC_NAME].valueSchema;
      const keySchemas = sr.schemas[TOPIC_NAME].keySchema;

      expect(typeof sr).toBe('object');
      expect(typeof valueSchemas).toBe('object');
      expect(typeof keySchemas).toBe('object');
      expect(keySchemas[2]).toBeTruthy();
      expect(keySchemas[2]).toHaveProperty('version');
      expect(keySchemas[2]).toHaveProperty('schema');
    });

    test('When a schema doesnt exist', async () => {
      expect.assertions(1);

      const sr = new SchemaRegistry(SCHEMA_REGISTRY_URL, ['fail-topic'], 'latest');

      await expect(sr.load()).rejects.toThrowError();
    });
  });

  afterAll(() => cleanupSchemas());
});
