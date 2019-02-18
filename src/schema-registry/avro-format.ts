import { Type } from 'avsc';

import { SchemaMap } from '../core/load-schemas';

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
  data: Object,
  magicByte: MagicByte = MAGIC_BYTE
) => {
  try {
    if (!data || !Object.keys(data).length) {
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

export const decodeAvroChunk = (
  allSchemas: Map<string, Map<number, SchemaMap>>,
  schemaType: SchemaFetchType,
  data: Buffer
) => {
  const schemas = <Map<number, SchemaMap>>allSchemas.get(schemaType);

  if (schemas) {
    const schemaIds = schemas.keys();

    for (let i = 0; i <= schemas.size && schemas.size; i += 1) {
      const schemaId = schemaIds.next();
      const schema = <SchemaMap>schemas.get(schemaId.value);

      try {
        const decoded = decodeAvro(schema.schema, schemaId.value, data);

        return <DecodedAvroChunk>{
          decoded,
          schemaId: schemaId.value,
        };
      } catch (error) {
        if (schemaId.done) {
          throw error;
        } else {
          continue;
        }
      }
    }
  }

  throw new TypeError('Failed to find schema size!');
};
