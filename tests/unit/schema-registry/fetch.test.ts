import nock from 'nock';

import { fetchSchema } from '../../../src/schema-registry';

nock('http://mock-schema-registry:1234')
  .persist()
  .get('/subjects/success-topic-value/versions/latest')
  .reply(200, {
    id: 263,
    subject: 'success-topic-value',
    version: 2,
    schema: '{"name":"traceToken","type":"string"}',
  })
  .persist()
  .get('/subjects/success-topic-value/versions/4')
  .reply(200, {
    id: 263,
    subject: 'success-topic-value',
    version: 4,
    schema: '{"name":"traceToken","type":"string"}',
  })
  .persist()
  .get('/subjects/success-bad-response-topic-value/versions/latest')
  .reply(204)
  .persist()
  .get('/subjects/fail-topic-value/versions/latest')
  .reply(404);

describe('Unit Test : src/schema-registry/fetch.ts', () => {
  describe('fetchSchema', () => {
    test('When a schema exists', async () => {
      expect.assertions(5);

      const response =
        await fetchSchema('http://mock-schema-registry:1234', 'success-topic', true, 'value');

      expect(response).toHaveProperty('id', 263);
      expect(response).toHaveProperty('subject', 'success-topic-value');
      expect(response).toHaveProperty('version', 2);
      expect(response).toHaveProperty(['schema', 'name'], 'traceToken');
      expect(response).toHaveProperty(['schema', 'type'], 'string');
    });

    test('When a schema exists with schema registry url ending with `/`', async () => {
      expect.assertions(5);

      const response =
        await fetchSchema('http://mock-schema-registry:1234/', 'success-topic', true, 'value');

      expect(response).toHaveProperty('id', 263);
      expect(response).toHaveProperty('subject', 'success-topic-value');
      expect(response).toHaveProperty('version', 2);
      expect(response).toHaveProperty(['schema', 'name'], 'traceToken');
      expect(response).toHaveProperty(['schema', 'type'], 'string');
    });

    test('When a schema exists with specified schema version', async () => {
      expect.assertions(5);

      const response =
        await fetchSchema('http://mock-schema-registry:1234/', 'success-topic', 4, 'value');

      expect(response).toHaveProperty('id', 263);
      expect(response).toHaveProperty('subject', 'success-topic-value');
      expect(response).toHaveProperty('version', 4);
      expect(response).toHaveProperty(['schema', 'name'], 'traceToken');
      expect(response).toHaveProperty(['schema', 'type'], 'string');
    });

    test('When a schema exists but response is malformed', async () => {
      expect.assertions(3);

      const response = await fetchSchema(
        'http://mock-schema-registry:1234', 'success-bad-response-topic', true, 'value'
      );

      expect(response).toHaveProperty(['httpResponse', 'status'], 204);
      expect(response).toHaveProperty(
        ['httpResponse', 'path'],
        '/subjects/success-bad-response-topic-value/versions/latest'
      );
      expect(response).toHaveProperty(['httpResponse', 'data'], '');
    });

    test('When a schema doesnt exist', async () => {
      expect.assertions(1);

      await expect(
        fetchSchema('http://mock-schema-registry:1234', 'fail-topic', true, 'value')
      ).rejects.toThrowError();
    });
  });
});
