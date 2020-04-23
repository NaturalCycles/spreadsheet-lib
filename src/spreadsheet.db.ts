import {
  CommonDB,
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  CommonSchema,
  CommonSchemaGenerator,
  DBQuery,
  DBTransaction,
  queryInMemory,
  RunQueryResult,
  SavedDBEntity,
} from '@naturalcycles/db-lib'
import { StringMap, _by, _Memo, _uniq } from '@naturalcycles/js-lib'
import { Debug, readableCreate, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { google, sheets_v4 } from 'googleapis'

export interface GCPServiceAccount {
  // keyFilename: string
  // project_id: string
  client_email: string
  private_key: string
  // [k: string]: any
}

export interface SpreadsheetDBCfg {
  gcpServiceAccount: GCPServiceAccount
  spreadsheetId: string
}

const log = Debug('nc:spreadsheet-lib')

export class SpreadsheetDB implements CommonDB {
  constructor(public cfg: SpreadsheetDBCfg) {}

  @_Memo()
  sheets(): sheets_v4.Sheets {
    const auth = new google.auth.JWT(
      this.cfg.gcpServiceAccount.client_email,
      undefined,
      this.cfg.gcpServiceAccount.private_key,
      [
        'https://www.googleapis.com/auth/spreadsheets',
        // 'https://www.googleapis.com/auth/drive'
        // 'https://www.googleapis.com/auth/drive.file'
        // 'https://www.googleapis.com/auth/drive.readonly'
        // 'https://www.googleapis.com/auth/spreadsheets'
        // 'https://www.googleapis.com/auth/spreadsheets.readonly'
      ],
    )
    google.options({ auth })

    return google.sheets({ version: 'v4', auth })
  }

  async ping(): Promise<void> {
    await this.getTableProperties()
  }

  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<DBM[]> {
    const dbmById = _by(await this.getAllRows<DBM>(table), 'id')
    return ids.map(id => dbmById[id]!).filter(Boolean)
  }

  async getAllRows<DBM extends SavedDBEntity>(table: string): Promise<DBM[]> {
    const res = await this.sheets().spreadsheets.get({
      spreadsheetId: this.cfg.spreadsheetId,
      ranges: [table],
      includeGridData: true,
    })
    if (!res.data.sheets?.[0].data?.[0]?.rowData?.length) return []

    const rows = res.data.sheets![0].data![0].rowData!
    const cols = rows[0].values!.map(v => v.effectiveValue?.stringValue!).filter(Boolean)
    // console.log(cols)

    const dbms: DBM[] = []

    rows.slice(1).forEach(r => {
      if (!r.values) return
      const dbm = {} as DBM
      r.values.forEach((cell, i) => {
        const v = cell.effectiveValue
        if (v && cols[i]) dbm[cols[i]] = v.boolValue ?? v.numberValue ?? v.stringValue
      })
      if (dbm.id) dbms.push(dbm)
    })

    return dbms
  }

  async getColumnNames(table: string): Promise<string[]> {
    const res = await this.sheets().spreadsheets.values.get({
      spreadsheetId: this.cfg.spreadsheetId,
      range: `${table}!A1:Z1`,
    })
    return res.data.values?.[0] || []
  }

  async saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opt: CommonDBSaveOptions = {},
  ): Promise<void> {
    // ensure table exists
    const { sheetId } = await this.createTableIfNeeded(table)

    // sync schema
    const cols = await this.getColumnNames(table)

    const neededCols = new Set<string>()
    dbms.forEach(dbm => Object.keys(dbm).forEach(col => neededCols.add(col)))
    const missingCols = [...neededCols].filter(col => !cols.includes(col))

    if (missingCols.length) {
      await this.sheets().spreadsheets.values.update({
        spreadsheetId: this.cfg.spreadsheetId,
        range: `${table}!A${cols.length + 1}`,
        valueInputOption: 'RAW',
        requestBody: {
          values: [missingCols],
        },
      })

      cols.push(...missingCols)

      log(`added columns: ${missingCols.join(', ')}`)
    }

    // load all ids, to know which rows to update
    const res = await this.sheets().spreadsheets.values.get({
      spreadsheetId: this.cfg.spreadsheetId,
      range: `${table}!A2:A1000`,
    })

    const rowById = {} as StringMap<number>
    ;(res.data.values || []).forEach(([id], i) => (rowById[id] = i + 1))
    const nextRow = (res.data.values?.length || 0) + 2

    // console.log(res.data.values)

    const dbmsToUpdate = dbms.filter(dbm => !!rowById[dbm.id])
    const dbmsToAppend = dbms.filter(dbm => !rowById[dbm.id])
    // console.log({
    //   dbmsToUpdate: dbmsToUpdate.length,
    //   dbmsToAppend: dbmsToAppend.length,
    // })

    if (dbmsToUpdate.length) {
      await this.sheets().spreadsheets.batchUpdate({
        spreadsheetId: this.cfg.spreadsheetId,

        requestBody: {
          requests: dbmsToUpdate.map(dbm => ({
            updateCells: {
              fields: '*',
              start: {
                rowIndex: rowById[dbm.id],
                columnIndex: 1,
                sheetId,
              },
              rows: [
                {
                  values: cols.slice(1).map(col => this.asCellData(dbm[col])),
                },
              ],
            },
          })),
        },
      })
    }

    if (dbmsToAppend.length) {
      await this.sheets().spreadsheets.values.append({
        spreadsheetId: this.cfg.spreadsheetId,
        range: `${table}!A${nextRow}`,
        valueInputOption: 'RAW',
        requestBody: {
          values: dbmsToAppend.map(dbm => cols.map(col => dbm[col])),
        },
      })
    }
  }

  async getRowByIdMap(table: string): Promise<StringMap<number>> {
    // load all ids, to know which rows to update
    const res = await this.sheets().spreadsheets.values.get({
      spreadsheetId: this.cfg.spreadsheetId,
      range: `${table}!A2:A1000`,
    })

    const rowById = {} as StringMap<number>
    ;(res.data.values || []).forEach(([id], i) => (rowById[id] = i + 1))
    return rowById
  }

  async deleteByIds(table: string, ids: string[], opt?: CommonDBOptions): Promise<number> {
    const { sheetId } = await this.createTableIfNeeded(table)

    const rowById = await this.getRowByIdMap(table)
    const existingIds = _uniq(ids).filter(id => !!rowById[id])
    if (!existingIds.length) return 0

    await this.sheets().spreadsheets.batchUpdate({
      spreadsheetId: this.cfg.spreadsheetId,
      requestBody: {
        // reverse, because when deleted, rows get "shifted up"
        requests: existingIds.reverse().map(id => ({
          deleteRange: {
            range: {
              sheetId,
              startRowIndex: rowById[id],
              endRowIndex: rowById[id]! + 1,
              // startColumnIndex: 0,
              // endColumnIndex: 999,
            },
            shiftDimension: 'ROWS',
          },
        })),
      },
    })

    return existingIds.length
  }

  async deleteByQuery(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    const { records } = await this.runQuery(q.select(['id']), opt)
    const ids = records.map(r => r.id)

    return await this.deleteByIds(q.table, ids, opt)
  }

  private asCellData(v: any): sheets_v4.Schema$CellData {
    // console.log(v, typeof v)
    if (typeof v === 'boolean') {
      return {
        userEnteredValue: {
          boolValue: v,
        },
      }
    }

    if (typeof v === 'number') {
      return {
        userEnteredValue: {
          numberValue: v,
        },
      }
    }

    return {
      userEnteredValue: {
        stringValue: v ? String(v) : undefined,
      },
    }
  }

  async resetCache(table?: string): Promise<void> {}

  async runQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    const rows = await this.getAllRows<DBM>(q.table)

    return {
      records: queryInMemory<DBM, OUT>(q, rows),
    }
  }

  async runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    const { records } = await this.runQuery(q, opt)
    return records.length
  }

  streamQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBStreamOptions,
  ): ReadableTyped<OUT> {
    const readable = readableCreate<DBM>()

    void this.runQuery(q, opt).then(({ records }) => {
      records.forEach(r => readable.push(r))
      readable.push(null) // done
    })

    return readable
  }

  async getTableSchema<DBM extends SavedDBEntity>(table: string): Promise<CommonSchema<DBM>> {
    const rows = await this.getAllRows(table)
    return CommonSchemaGenerator.generateFromRows(
      {
        table,
      },
      rows,
    )
  }

  async getTables(): Promise<string[]> {
    return Object.keys(await this.getTableProperties())
  }

  async getTableProperties(): Promise<StringMap<sheets_v4.Schema$SheetProperties>> {
    const res = await this.sheets().spreadsheets.get({
      spreadsheetId: this.cfg.spreadsheetId,
    })

    const records = (res.data.sheets || []).map(s => s.properties!)
    return _by(records, 'title')
  }

  async createTableIfNeeded(table: string): Promise<sheets_v4.Schema$SheetProperties> {
    const propsMap = await this.getTableProperties()
    if (propsMap[table]) return propsMap[table]!

    const res = await this.sheets().spreadsheets.batchUpdate({
      spreadsheetId: this.cfg.spreadsheetId,
      requestBody: {
        requests: [
          {
            addSheet: {
              properties: {
                gridProperties: {
                  frozenColumnCount: 1,
                  frozenRowCount: 1,
                },
                title: table,
              },
            },
          },
        ],
      },
    })

    // console.log(res.data.replies)
    log(`created table ${table}`)

    return res.data.replies![0].addSheet!.properties!
  }

  async deleteTableIfExists(table: string): Promise<void> {
    const sheetId = (await this.getTableProperties())[table]?.sheetId
    if (!sheetId) return

    await this.sheets().spreadsheets.batchUpdate({
      spreadsheetId: this.cfg.spreadsheetId,
      requestBody: {
        requests: [
          {
            deleteSheet: {
              sheetId,
            },
          },
        ],
      },
    })

    log(`deleted table ${table}`)
  }

  async createTable(schema: CommonSchema, opt?: CommonDBCreateOptions): Promise<void> {
    await this.createTableIfNeeded(schema.table)
    // column names will be added on-demand
  }

  transaction(): DBTransaction {
    return new DBTransaction(this)
  }
}
