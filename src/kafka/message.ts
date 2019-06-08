export interface Chunk {
  value: Buffer;
  key: Buffer;
  size: number;
  topic: string;
  offset: number;
  partition: number;
  timestamp: number;
}

export interface KafkaMessage {
  value: Buffer;
  key: Buffer;
  size: number;
  topic: string;
  offset: number;
  partition: number;
  timestamp: number;
}

export interface AvroMessage extends KafkaMessage {
  valueSchemaId: number;
  keySchemaId: number;
  parsedValue: JSON & string & number;
  parsedKey: JSON & string & number;
}
