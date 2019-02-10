module.exports = {
  preset: 'ts-jest/presets/js-with-ts',
  testEnvironment: 'node',
  collectCoverage: true,
  coverageDirectory: './coverage/',
  testMatch: [
    '**/tests/**/*.test.ts',
  ],
}
