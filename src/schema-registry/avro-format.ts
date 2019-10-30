import { Type } from 'avsc';

import { TypeSchemas } from './schema-registry';
import {
  encodeWireFormat,
  MAGIC_BYTE,
  WirePosition,
  SchemaId,
  MagicByte,
} from './wire-format';

export interface DecodedAvroChunk {
  decoded: JSON;
  schemaId: number;
}

export const encodeAvro = (
  schema: Type,
  schemaId: SchemaId,
  data: unknown,
  magicByte: MagicByte = MAGIC_BYTE
) => {
  try {
    if (!data || (typeof data === 'object' && !Object.keys(<Object>data).length)) {
      throw new TypeError('Data cannot be empty');
    }

    const bufData = schema.toBuffer(data);

    return encodeWireFormat(bufData, schemaId, magicByte);
  } catch (error) {
    throw error;
  }
};

export const decodeAvro = (
  schema: Type,
  schemaId: SchemaId,
  data: Buffer,
  magicByte: MagicByte = MAGIC_BYTE
) => {
  try {
    if (!data || !data.length) {
      throw new TypeError('Data cannot be empty');
    }

    const readMagicByte = data[0];
    if (readMagicByte !== magicByte) {
      throw new TypeError('Data has incorrect magic byte or is unserialized');
    }

    const readSchemaId = data.readInt32BE(WirePosition.SchemaId);
    if (readSchemaId !== Number(schemaId)) {
      throw new TypeError('Data has incorrect schema id or is unserialized');
    }

    const readAvroData = data.slice(WirePosition.Data, data.length);

    return schema.fromBuffer(readAvroData);
  } catch (error) {
    throw error;
  }
};

export const encodeAvroChunk = (
  schemas: TypeSchemas,
  data?: unknown
) => {
  if (!data || (typeof data === 'object' && !Object.keys(<Object>data).length)) {
    return Buffer.alloc(0);
  }

  let err = null;

  if (schemas && Object.keys(<Object>schemas).length) {
    for (const schemaId in schemas) {
      try {
        return encodeAvro(schemas[schemaId].schema, schemaId, data);
      } catch (error) {
        err = error;
        continue;
      }
    }
  }

  if (err) {
    throw err;
  }

  throw new TypeError('Failed to find schema size!');
};

export const decodeAvroChunk = (
  schemas: TypeSchemas,
  data: Buffer
) => {
  let err = null;

  if (schemas && Object.keys(<Object>schemas).length) {
    for (const schemaId in schemas) {
      try {
        const decoded = decodeAvro(schemas[schemaId].schema, schemaId, data);

        return <DecodedAvroChunk>{
          decoded,
          schemaId: Number.parseInt(schemaId, 10),
        };
      } catch (error) {
        err = error;
        continue;
      }
    }
  }

  if (err) {
    throw err;
  }

  throw new TypeError('Failed to find schema size!');
};
