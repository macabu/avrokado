import nock from 'nock';

import { SchemaRegistry } from '../../../src/schema-registry';

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
  .get('/subjects/fail-topic-key/versions/latest')
  .reply(404)
  .persist()
  .get('/subjects/success-topic-value/versions')
  .reply(200, [2, 1])
  .persist()
  .get('/subjects/success-bad-response-topic-value/versions')
  .reply(204)
  .persist();

describe('Unit Test : src/schema-registry/load-schemas.ts', () => {
  describe('loadSchemas', () => {
    test('Loading the latest value and key schema version', async () => {
      expect.assertions(6);

      const sr = new SchemaRegistry('http://mock-schema-registry:1234', 'success-topic', 'latest');

      await sr.load();

      const valueSchemas = sr.schemas['success-topic'].valueSchema;
      const keySchemas = sr.schemas['success-topic'].keySchema;

      expect(typeof sr).toBe('object');
      expect(typeof valueSchemas).toBe('object');
      expect(typeof keySchemas).toBe('object');
      expect(keySchemas[3]).toBeTruthy();
      expect(keySchemas[3]).toHaveProperty('version', 1);
      expect(keySchemas[3]).toHaveProperty('schema');
    });

    test('When a schema doesnt exist', async () => {
      expect.assertions(1);

      const sr = new SchemaRegistry('http://mock-schema-registry:1234', ['fail-topic'], 'latest');

      await expect(sr.load()).rejects.toThrowError();
    });
  });
});
