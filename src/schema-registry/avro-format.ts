import { Type } from 'avsc';

import { SchemaObject } from '../core/load-schemas';

import {
  encodeWireFormat,
  MAGIC_BYTE,
  WirePosition,
  SchemaId,
  MagicByte,
  Data,
} from './wire-format';

export interface DecodedAvroChunk {
  decoded: JSON;
  schemaId: number;
}

type SchemaFetchType = 'key' | 'value';

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

    return <Data>encodeWireFormat(bufData, schemaId, magicByte);
  } catch (error) {
    throw error;
  }
};

export const decodeAvro = (
  schema: Type,
  schemaId: SchemaId,
  data: Data,
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
    if (readSchemaId !== schemaId) {
      throw new TypeError('Data has incorrect schema id or is unserialized');
    }

    const readAvroData = data.slice(WirePosition.Data, data.length);

    return schema.fromBuffer(readAvroData);
  } catch (error) {
    throw error;
  }
};

export const encodeAvroChunk = (
  allSchemas: Map<string, SchemaObject[]>,
  schemaType: SchemaFetchType,
  data?: unknown
) => {
  if (!data || (typeof data === 'object' && !Object.keys(<Object>data).length)) {
    return Buffer.alloc(0);
  }

  const schemas = <SchemaObject[]>allSchemas.get(schemaType);

  if (schemas && schemas.length) {
    for (let i = schemas.length; i >= 0; i -= 1) {
      try {
        return <Data>encodeAvro(schemas[i].schema, i, data);
      } catch (error) {
        if (i === 0) {
          throw error;
        } else {
          continue;
        }
      }
    }
  }

  throw new TypeError('Failed to find schema size!');
};

export const decodeAvroChunk = (
  allSchemas: Map<string, SchemaObject[]>,
  schemaType: SchemaFetchType,
  data: Buffer
) => {
  const schemas = <SchemaObject[]>allSchemas.get(schemaType);

  if (schemas && schemas.length) {
    for (let i = schemas.length; i >= 0; i -= 1) {
      try {
        const decoded = decodeAvro(schemas[i].schema, i, data);

        return <DecodedAvroChunk>{
          decoded,
          schemaId: i,
        };
      } catch (error) {
        if (i === 0) {
          throw error;
        } else {
          continue;
        }
      }
    }
  }

  throw new TypeError('Failed to find schema size!');
};
