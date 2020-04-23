## @naturalcycles/spreadsheet-lib

> [CommonDB](https://github.com/NaturalCycles/db-lib) implementation backed by a Google Spreadsheet

[![npm](https://img.shields.io/npm/v/@naturalcycles/spreadsheet-lib/latest.svg)](https://www.npmjs.com/package/@naturalcycles/spreadsheet-lib)
[![](https://circleci.com/gh/NaturalCycles/spreadsheet-lib.svg?style=shield&circle-token=123)](https://circleci.com/gh/NaturalCycles/spreadsheet-lib)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)

# Example

```typescript
// Setup SpreadsheetDB
const db = new SpreadsheetDB({
  gcpServiceAccount: {
    client_email: 'lalala@lololo.iam.gserviceaccount.com'
    private_key: 'verysecret',
  },
  spreadsheetId: '17meRABNrr4Pik9FF5HRQgRxxi4kjY2_dCHg3k2nqGGE',
})

// Use it as CommonDB
const items = await db.getByIds('TEST_TABLE', ['id1', 'id2'])
// ...
```
