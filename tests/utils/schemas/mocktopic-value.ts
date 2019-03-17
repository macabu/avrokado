export const valueSchema = {
  type: 'record',
  name: 'dogsInfo',
  doc: 'Information about dogs',
  fields: [
    {
      name: 'name',
      type: 'string',
      doc: 'Name of the dog',
    },
    {
      name: 'age',
      type: 'int',
      doc: 'Age of the dog',
    },
    {
      name: 'breed',
      type: {
        type: 'enum',
        name: 'breedEnum',
        doc: 'Enum for the dog breeds',
        symbols: [
          'Beagle',
          'Maltese',
          'Other',
        ],
      },
      doc: 'Breed of the dog',
    },
    {
      name: 'owner',
      type: [
        'string',
        'null',
      ],
      doc: 'Name of the owner of the dog. Can be null',
    },
    {
      name: 'createdAt',
      type: 'long',
      doc: 'Timestamp when the message was created on Kafka',
      logicalType: 'timestamp-millis',
    },
  ],
};
