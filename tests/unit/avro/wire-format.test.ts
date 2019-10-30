import { encodeWireFormat } from '../../../src/avro/wire-format';

describe('Unit Test : src/schema-registry/wire-format.ts', () => {
  describe('encodeWireFormat', () => {
    test('With valid sized data and schema id',  () => {
      expect.assertions(4);

      const data = Buffer.from('A test string');
      const schemaId = 3;

      const buf = encodeWireFormat(data, schemaId);

      expect(buf).toHaveLength(18);
      expect(buf[0]).toBe(0x0);
      expect(buf.readInt32BE(1)).toBe(schemaId);
      expect(buf.toString()).toContain(data);
    });

    test('With valid sized data, schema id and magic byte',  () => {
      expect.assertions(4);

      const data = Buffer.from('A test string');
      const schemaId = 3;
      const magicByte = 0x1;

      const buf = encodeWireFormat(data, schemaId, magicByte);

      expect(buf).toHaveLength(18);
      expect(buf[0]).toBe(magicByte);
      expect(buf.readInt32BE(1)).toBe(schemaId);
      expect(buf.toString()).toContain(data);
    });

    test('With valid sized data, schema id and magic byte as strings',  () => {
      expect.assertions(4);

      const data = Buffer.from('A test string');
      const schemaId = '3';
      const magicByte = '0x1';

      const buf = encodeWireFormat(data, schemaId, magicByte);

      expect(buf).toHaveLength(18);
      expect(buf[0]).toBe(Number.parseInt(magicByte, 16));
      expect(buf.readInt32BE(1)).toBe(Number.parseInt(schemaId, 10));
      expect(buf.toString()).toContain(data);
    });

    test('With empty data',  () => {
      const data = Buffer.alloc(0);
      const schemaId = 3;

      expect(() => {
        encodeWireFormat(data, schemaId);
      }).toThrow(TypeError);
    });

    test('With large data',  () => {
      const data = Buffer.alloc(9000);
      const schemaId = 3;

      expect(() => {
        encodeWireFormat(data, schemaId);
      }).toThrow(TypeError);
    });
  });
});
