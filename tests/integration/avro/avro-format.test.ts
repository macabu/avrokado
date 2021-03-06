import nock from 'nock';
import { Type, Schema } from 'avsc';

import {
  encodeAvro,
  decodeAvro,
  encodeAvroChunk,
  decodeAvroChunk,
} from '../../../src/avro/avro-format';
import { SchemaRegistry } from '../../../src/schema-registry';

nock('http://mock-schema-registry:1234')
  .persist()
  .get('/subjects/success-topic-value/versions/latest')
  .reply(200, {
    id: 263,
    subject: 'success-topic-value',
    version: 2,
    schema: '{ "type": "record", "fields": [{"name": "name", "type": "string"}] }',
  })
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
    id: 261,
    subject: 'success-topic-value',
    version: 1,
    schema: '{ "type": "record", "fields": [{"name": "age", "type": "int"}] }',
  })
  .persist()
  .get('/subjects/success-topic-key/versions/latest')
  .reply(200, {
    id: 262,
    subject: 'success-topic-key',
    version: 1,
    schema: '"string"',
  })
  .persist()
  .get('/subjects/success-topic-key/versions/1')
  .reply(200, {
    id: 262,
    subject: 'success-topic-key',
    version: 1,
    schema: '"string"',
  })
  .persist()
  .get('/subjects/success-topic-value/versions')
  .reply(200, [2, 1])
  .persist()
  .get('/subjects/success-topic-key/versions')
  .reply(200, [1]);

describe('Integration Test : src/schema-registry/avro-format.ts', () => {
  describe('encodeAvro', () => {
    test('With valid data', () => {
      expect.assertions(3);

      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const schemaId = 3;

      const data = {
        kind: 'DOG',
        name: 'Kiara',
      };

      const magicByte = 0x0;

      const encoded = encodeAvro(schema, schemaId, data, magicByte);

      expect(encoded).toHaveLength(12);
      expect(encoded[0]).toBe(magicByte);
      expect(encoded.readInt32BE(1)).toBe(schemaId);
    });

    test('With valid data and default magic byte', () => {
      expect.assertions(3);

      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const schemaId = 3;

      const data = {
        kind: 'DOG',
        name: 'Kiara',
      };

      const encoded = encodeAvro(schema, schemaId, data);

      expect(encoded).toHaveLength(12);
      expect(encoded[0]).toBe(0x0);
      expect(encoded.readInt32BE(1)).toBe(schemaId);
    });

    test('With empty data', () => {
      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const schemaId = 3;

      const data = {};

      const magicByte = 0x0;

      expect(() => {
        encodeAvro(schema, schemaId, data, magicByte);
      }).toThrow(TypeError);
    });

    test('With data that doesnt match the schema', () => {
      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const schemaId = 3;

      const data = {
        age: 1,
        nickname: 'Doggo',
      };

      const magicByte = 0x0;

      expect(() => {
        encodeAvro(schema, schemaId, data, magicByte);
      }).toThrow(Error);
    });
  });

  describe('decodeAvro', () => {
    test('With valid data', () => {
      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const schemaId = 3;

      const data = {
        kind: 'DOG',
        name: 'Kiara',
      };

      const magicByte = 0x0;

      const encoded = encodeAvro(schema, schemaId, data, magicByte);

      const decoded = decodeAvro(schema, schemaId, encoded, magicByte);

      expect(decoded).toMatchObject({
        kind: 'DOG',
        name: 'Kiara',
      });
    });

    test('With valid data and default magic byte', () => {
      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const schemaId = 3;

      const data = {
        kind: 'DOG',
        name: 'Kiara',
      };

      const encoded = encodeAvro(schema, schemaId, data);

      const decoded = decodeAvro(schema, schemaId, encoded);

      expect(decoded).toMatchObject({
        kind: 'DOG',
        name: 'Kiara',
      });
    });

    test('With empty data', () => {
      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const schemaId = 3;

      const data = Buffer.alloc(0);

      const magicByte = 0x0;

      expect(() => {
        decodeAvro(schema, schemaId, data, magicByte);
      }).toThrow(TypeError);
    });

    test('With wrong magic byte', () => {
      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const schemaId = 3;

      const data = {
        kind: 'DOG',
        name: 'Kiara',
      };

      const encodeMagicByte = 0x0;
      const decodeMagicByte = 0x1;

      const encoded = encodeAvro(schema, schemaId, data, encodeMagicByte);

      expect(() => {
        decodeAvro(schema, schemaId, encoded, decodeMagicByte);
      }).toThrow(TypeError);
    });

    test('With wrong schema id', () => {
      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const encodeSchemaId = 3;
      const decodeSchemaId = 4;

      const data = {
        kind: 'DOG',
        name: 'Kiara',
      };

      const magicByte = 0x0;

      const encoded = encodeAvro(schema, encodeSchemaId, data, magicByte);

      expect(() => {
        decodeAvro(schema, decodeSchemaId, encoded, magicByte);
      }).toThrow(TypeError);
    });

    test('With decode schema longer than encode schema', () => {
      const encodeSchema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const decodeSchema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'age',
            type: 'int',
          },
          {
            name: 'alive',
            type: 'boolean',
          },
          {
            name: 'nickname',
            type: 'string',
          },
        ],
      }));

      const schemaId = 3;

      const data = {
        kind: 'DOG',
        name: 'Kiara',
      };

      const magicByte = 0x0;

      const encoded = encodeAvro(encodeSchema, schemaId, data, magicByte);

      expect(() => {
        decodeAvro(decodeSchema, schemaId, encoded, magicByte);
      }).toThrow();
    });

    test('With decode schema shorter than encode schema', () => {
      const encodeSchema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const decodeSchema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'age',
            type: 'int',
          },
        ],
      }));

      const schemaId = 3;

      const data = {
        kind: 'DOG',
        name: 'Kiara',
      };

      const magicByte = 0x0;

      const encoded = encodeAvro(encodeSchema, schemaId, data, magicByte);

      expect(() => {
        decodeAvro(decodeSchema, schemaId, encoded, magicByte);
      }).toThrow();
    });

    test('With decode schema with different field types than encode schema', () => {
      const encodeSchema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: {
              type: 'enum',
              symbols: ['CAT', 'DOG'],
            },
          },
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const decodeSchema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'kind',
            type: 'boolean',
          },
          {
            name: 'name',
            type: 'long',
          },
        ],
      }));

      const schemaId = 3;

      const data = {
        kind: 'DOG',
        name: 'Kiara',
      };

      const magicByte = 0x0;

      const encoded = encodeAvro(encodeSchema, schemaId, data, magicByte);

      expect(() => {
        decodeAvro(decodeSchema, schemaId, encoded, magicByte);
      }).toThrow();
    });
  });

  describe('decodeAvroChunk', () => {
    test('With valid data using latest schema', async () => {
      expect.assertions(2);

      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'name',
            type: 'string',
          },
        ],
      }));

      const schemaId = 263;

      const data = {
        name: 'Kiara',
      };

      const magicByte = 0x0;

      const sr = new SchemaRegistry(
        'http://mock-schema-registry:1234',
        'success-topic',
        'latest'
      );

      await sr.load();

      const encoded = encodeAvro(schema, schemaId, data, magicByte);

      const { decoded, schemaId: dSchemaId } =
        decodeAvroChunk(sr.schemas['success-topic'].valueSchema, encoded);

      expect(decoded).toMatchObject(data);
      expect(dSchemaId).toBe(schemaId);
    });

    test('With valid data using all schemas', async () => {
      expect.assertions(2);

      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'age',
            type: 'int',
          },
        ],
      }));

      const schemaId = 261;

      const data = {
        age: 1,
      };

      const magicByte = 0x0;

      const sr = new SchemaRegistry(
        'http://mock-schema-registry:1234',
        'success-topic',
        'all'
      );

      await sr.load();

      const encoded = encodeAvro(schema, schemaId, data, magicByte);

      const { decoded, schemaId: dSchemaId } =
        decodeAvroChunk(sr.schemas['success-topic'].valueSchema, encoded);

      expect(decoded).toMatchObject(data);
      expect(dSchemaId).toBe(schemaId);
    });

    test('With invalid data (magic byte)', async () => {
      expect.assertions(1);

      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'age',
            type: 'int',
          },
        ],
      }));

      const schemaId = 261;

      const data = {
        age: 1,
      };

      const magicByte = 0x1;

      const sr = new SchemaRegistry(
        'http://mock-schema-registry:1234',
        'success-topic',
        'all'
      );

      sr.load();

      const encoded = encodeAvro(schema, schemaId, data, magicByte);

      expect(() => {
        decodeAvroChunk(sr.schemas['success-topic'].valueSchema, encoded);
      }).toThrow();
    });

    test('With no schemas', async () => {
      expect.assertions(1);

      const schema = Type.forSchema(<Schema>({
        type: 'record',
        fields: [
          {
            name: 'age',
            type: 'int',
          },
        ],
      }));

      const schemaId = 261;

      const data = {
        age: 1,
      };

      const magicByte = 0x0;

      const encoded = encodeAvro(schema, schemaId, data, magicByte);

      expect(() => {
        decodeAvroChunk({}, encoded);
      }).toThrow(TypeError);
    });
  });

  describe('encodeAvroChunk', () => {
    test('With valid data using latest schema', async () => {
      expect.assertions(5);

      const data = {
        name: 'Kiara',
      };

      const sr = new SchemaRegistry(
        'http://mock-schema-registry:1234',
        'success-topic',
        'latest'
      );

      await sr.load();

      const encodedValue =
        encodeAvroChunk(sr.schemas['success-topic'].valueSchema, data);

      expect(encodedValue).toBeTruthy();
      expect(typeof encodedValue).toBe('object');
      expect(Buffer.isBuffer(encodedValue)).toBeTruthy();
      expect(encodedValue[0]).toBe(0);
      expect(encodedValue.readInt32BE(1)).toBe(263);
    });

    test('With valid data using all schemas', async () => {
      expect.assertions(5);

      const data = {
        age: 1,
      };

      const sr = new SchemaRegistry(
        'http://mock-schema-registry:1234',
        'success-topic',
        'all'
      );

      await sr.load();

      const encodedValue =
        encodeAvroChunk(sr.schemas['success-topic'].valueSchema, data);

      expect(encodedValue).toBeTruthy();
      expect(typeof encodedValue).toBe('object');
      expect(Buffer.isBuffer(encodedValue)).toBeTruthy();
      expect(encodedValue[0]).toBe(0);
      expect(encodedValue.readInt32BE(1)).toBe(261);
    });

    test('With no data', async () => {
      expect.assertions(1);

      const encodedValue = encodeAvroChunk({});

      expect(encodedValue).toHaveLength(0);
    });

    test('With no schemas', async () => {
      expect.assertions(1);

      const data = {
        age: 1,
      };

      expect(() => {
        encodeAvroChunk({}, data);
      }).toThrow(TypeError);
    });

    test('With invalid data', async () => {
      expect.assertions(1);

      const data = 'Kiara';

      const sr = new SchemaRegistry(
        'http://mock-schema-registry:1234',
        'success-topic',
        'all'
      );

      await sr.load();

      expect(() => {
        encodeAvroChunk(sr.schemas['success-topic'].valueSchema, data);
      }).toThrow();
    });
  });
});
