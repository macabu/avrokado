import { uploadSchema } from './create-schema';

describe('Unit Test : tests/utils/create-schema.ts', () => {
  describe('uploadSchema', () => {
    test('With invalid Schema Registry URL', async () => {
      expect.assertions(1);

      const result = await uploadSchema('this-wont-work', 'any-topic', 'schema', 'key');

      expect(result).toBeFalsy();
    });
  });
});
