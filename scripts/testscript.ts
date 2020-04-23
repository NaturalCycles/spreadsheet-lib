/*

DEBUG=nc* yarn tsn testscript

 */

import { requireEnvKeys } from '@naturalcycles/nodejs-lib'
import { runScript } from '@naturalcycles/nodejs-lib/dist/script'

runScript(async () => {
  const { GCP_CFG } = requireEnvKeys('GCP_CFG')
  const gcpCfg = JSON.parse(JSON.parse(GCP_CFG).gcpCfg)
  console.log(gcpCfg)

  // const db = new InMemoryDB()
  // // const db = new DatastoreDB()
  //
  // const items = createTestItemsDBM(5)
  //
  // await db.saveBatch(TEST_TABLE, items)
  //
  // // const r = await db.getByIds(TEST_TABLE, ['id1', 'asdsad'])
  // const q = new DBQuery(TEST_TABLE).order('k2', true)
  // const r = await db.runQuery(q)
  //
  // console.log(r)
})
