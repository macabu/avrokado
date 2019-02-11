import { Type, Schema } from 'avsc';

import { encodeAvro, decodeAvro } from '../../../src/schema-registry/avro-format';

describe('Integration Test : src/schema-registry/wire-format.ts', () => {
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
});
