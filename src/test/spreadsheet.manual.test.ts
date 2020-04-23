import {
  CommonDBImplementationFeatures,
  CommonDBImplementationQuirks,
  createTestItemsDBM,
  runCommonDBTest,
  TEST_TABLE,
} from '@naturalcycles/db-lib/dist/testing'
import { requireEnvKeys } from '@naturalcycles/nodejs-lib'
import { GCPServiceAccount, SpreadsheetDB } from '../spreadsheet.db'

jest.setTimeout(60000)

require('dotenv').config()

const { GCP_CFG } = requireEnvKeys('GCP_CFG')
const gcpServiceAccount = JSON.parse(JSON.parse(GCP_CFG).gcpCfg) as GCPServiceAccount

const db = new SpreadsheetDB({
  gcpServiceAccount,
  spreadsheetId: '17meRABNrr4Pik9FF5HRQgRxxi4kjY2_dCHg3k2nqGGE',
})

const features: CommonDBImplementationFeatures = {
  // strongConsistency: false,
}
const quirks: CommonDBImplementationQuirks = {
  // eventualConsistencyDelay: 100,
}

test('auth', async () => {
  // const r = await db.getColumnNames(TEST_TABLE)

  // const r = await db.getByIds<TestItemDBM>(TEST_TABLE, ['id2', 'id3'])
  // const r = await db.deleteByIds(TEST_TABLE, ['id5', 'id5'])
  // await db.createTableIfNeeded(TEST_TABLE + 's')
  // await db.deleteTableIfExists(TEST_TABLE)
  // console.log(r)
  await db.saveBatch(TEST_TABLE, createTestItemsDBM(5))
  // const r = await db.getByIds<TestItemDBM>(TEST_TABLE, ['id2'])
  // const { records } = await db.runQuery(new DBQuery(TEST_TABLE).select(['id']))
  // console.log(records)
  // const r = await db.runQueryCount(new DBQuery(TEST_TABLE))
  const r = await db.getTableSchema(TEST_TABLE)
  console.log(r)
})

describe('runCommonDBTest', () => runCommonDBTest(db, features, quirks))
