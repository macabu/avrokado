import nock from 'nock';

import { loadSchemasByType, loadSchemas } from '../../../src/core/load-schemas';

nock('http://mock-schema-registry:1234')
  .persist()
  .get('/subjects/success-topic-value/versions/latest')
  .reply(200, {
    id: 263,
    subject: 'success-topic-value',
    version: 2,
    schema: '{ "type": "record", "fields": [{"name": "name", "type": "boolean"}] }',
  })
  .persist()
  .get('/subjects/success-topic-value/versions/3')
  .reply(404)
  .persist()
  .get('/subjects/success-topic-value/versions/2')
  .reply(200, {
    id: 263,
    subject: 'success-topic-value',
    version: 2,
    schema: '{ "type": "record", "fields": [{"name": "name", "type": "string"}] }',
  })
  .persist()
  .get('/subjects/success-topic-value/versions/1')
  .reply(200, {
    id: 262,
    subject: 'success-topic-value',
    version: 1,
    schema: '{ "type": "record", "fields": [{"name": "myName", "type": "string"}] }',
  })
  .persist()
  .get('/subjects/success-topic-key/versions/latest')
  .reply(200, {
    id: 3,
    subject: 'success-topic-key',
    version: 1,
    schema: '"string"',
  })
  .persist()
  .get('/subjects/success-topic-key/versions/1')
  .reply(200, {
    id: 3,
    subject: 'success-topic-key',
    version: 1,
    schema: '"string"',
  })
  .persist()
  .get('/subjects/success-bad-response-topic-value/versions/latest')
  .reply(204)
  .persist()
  .get('/subjects/fail-topic-value/versions/latest')
  .reply(404)
  .persist()
  .get('/subjects/success-topic-value/versions')
  .reply(200, [2, 1])
  .persist()
  .get('/subjects/success-bad-response-topic-value/versions')
  .reply(204)
  .persist();

describe('Unit Test : src/core/load-schemas.ts', () => {
  describe('loadSchemasByType', () => {
    test('Loading the latest value schema version', async () => {
      expect.assertions(5);

      const response = await loadSchemasByType(
        'http://mock-schema-registry:1234',
        'success-topic',
        'latest',
        'value'
      );

      const schema = JSON.parse(response.get(263)!.schema as unknown as string);

      expect(typeof response).toBe('object');
      expect(response.has(263)).toBeTruthy();
      expect(response.get(263)).toHaveProperty('version', 2);
      expect(schema).toHaveProperty('type', 'record');
      expect(schema).toHaveProperty('fields');
    });

    test('Loading all value schema versions', async () => {
      expect.assertions(7);

      const response = await loadSchemasByType(
        'http://mock-schema-registry:1234',
        'success-topic',
        'all',
        'value'
      );

      const firstSchema = JSON.parse(response.get(262)!.schema as unknown as string);
      const latestSchema = JSON.parse(response.get(263)!.schema as unknown as string);

      expect(typeof response).toBe('object');
      expect(response.has(262)).toBeTruthy();
      expect(response.has(263)).toBeTruthy();
      expect(response.get(262)).toHaveProperty('version', 1);
      expect(response.get(263)).toHaveProperty('version', 2);
      expect(firstSchema).toHaveProperty('type', 'record');
      expect(latestSchema).toHaveProperty('type', 'record');
    });

    test('Loading a single value schema version', async () => {
      expect.assertions(5);

      const response = await loadSchemasByType(
        'http://mock-schema-registry:1234',
        'success-topic',
        2,
        'value'
      );

      const schema = JSON.parse(response.get(263)!.schema as unknown as string);

      expect(typeof response).toBe('object');
      expect(response.has(263)).toBeTruthy();
      expect(response.get(263)).toHaveProperty('version', 2);
      expect(schema).toHaveProperty('type', 'record');
      expect(schema).toHaveProperty('fields');
    });

    test('Loading a single value schema version which does not exist', async () => {
      expect.assertions(1);

      await expect(
        loadSchemasByType(
          'http://mock-schema-registry:1234',
          'success-topic',
          3,
          'value'
        )
      ).rejects.toThrowError();
    });
  });

  describe('loadSchemas', () => {
    test('Loading the latest value and key schema version', async () => {
      expect.assertions(6);

      const response = await loadSchemas(
        'http://mock-schema-registry:1234',
        'success-topic',
        'latest'
      );

      const valueSchemas = response.get('value')!;
      const keySchemas = response.get('key')!;

      expect(typeof response).toBe('object');
      expect(typeof valueSchemas).toBe('object');
      expect(typeof keySchemas).toBe('object');
      expect(keySchemas.has(3)).toBeTruthy();
      expect(keySchemas.get(3)).toHaveProperty('version', 1);
      expect(keySchemas.get(3)).toHaveProperty('schema');
    });

    test('When a schema doesnt exist', async () => {
      expect.assertions(1);

      await expect(
        loadSchemas('http://mock-schema-registry:1234', 'fail-topic', 'latest')
      ).rejects.toThrowError();
    });
  });
});
