export type MagicByte = number | string;
export type SchemaId = number | string;

enum BufferSize {
  MIN = 0,
  MAX = 8192,
}
export enum WirePosition {
  MagicByte = 0x0,
  SchemaId = 0x1,
  Data = 0x5,
}

export const MAGIC_BYTE = 0x0;

export const encodeWireFormat = (
  data: Buffer,
  schemaId: SchemaId,
  magicByte: MagicByte = MAGIC_BYTE
) => {
  try {
    if (!data || !data.length) {
      throw new TypeError('Data cannot be empty');
    }

    const inputsSize =
      WirePosition.MagicByte +
      WirePosition.SchemaId +
      WirePosition.Data -
      1;

    const concatBufSize = inputsSize + data.length;

    if (concatBufSize <= BufferSize.MIN || concatBufSize > BufferSize.MAX) {
      throw new TypeError('Buffer size is either too small or too big');
    }

    const saneMagicByte = (typeof magicByte === 'string')
      ? Number.parseInt(magicByte, 16)
      : magicByte;

    const saneSchemaId = (typeof schemaId === 'string')
      ? Number.parseInt(schemaId, 10)
      : schemaId;

    const buf = Buffer.alloc(inputsSize);

    buf[0] = saneMagicByte;
    buf.writeInt32BE(saneSchemaId, WirePosition.SchemaId);

    const concatBuf = Buffer.concat([buf, data], concatBufSize);

    return concatBuf;
  } catch (error) {
    throw error;
  }
};
