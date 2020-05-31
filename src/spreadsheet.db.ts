import {
  BaseCommonDB,
  CommonDB,
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  CommonSchema,
  CommonSchemaGenerator,
  DBQuery,
  ObjectWithId,
  queryInMemory,
  RunQueryResult,
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

export class SpreadsheetDB extends BaseCommonDB implements CommonDB {
  constructor(public cfg: SpreadsheetDBCfg) {
    super()
  }

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

  async getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<ROW[]> {
    const rowById = _by(await this.getAllRows<ROW>(table), 'id')
    return ids.map(id => rowById[id]!).filter(Boolean)
  }

  async getAllRows<ROW extends ObjectWithId>(table: string): Promise<ROW[]> {
    const res = await this.sheets().spreadsheets.get({
      spreadsheetId: this.cfg.spreadsheetId,
      ranges: [table],
      includeGridData: true,
    })
    if (!res.data.sheets?.[0].data?.[0]?.rowData?.length) return []

    const sheetRows = res.data.sheets![0].data![0].rowData!
    const cols = sheetRows[0].values!.map(v => v.effectiveValue?.stringValue!).filter(Boolean)
    // console.log(cols)

    const outputRows: ROW[] = []

    sheetRows.slice(1).forEach(sheetRow => {
      if (!sheetRow.values) return
      const row = {} as ROW
      sheetRow.values.forEach((cell, i) => {
        const v = cell.effectiveValue
        if (v && cols[i]) row[cols[i]] = v.boolValue ?? v.numberValue ?? v.stringValue
      })
      if (row.id) outputRows.push(row)
    })

    return outputRows
  }

  async getColumnNames(table: string): Promise<string[]> {
    const res = await this.sheets().spreadsheets.values.get({
      spreadsheetId: this.cfg.spreadsheetId,
      range: `${table}!A1:Z1`,
    })
    return res.data.values?.[0] || []
  }

  async saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
    opt: CommonDBSaveOptions = {},
  ): Promise<void> {
    // ensure table exists
    const { sheetId } = await this.createTableIfNeeded(table)

    // sync schema
    const cols = await this.getColumnNames(table)

    const neededCols = new Set<string>()
    rows.forEach(r => Object.keys(r).forEach(col => neededCols.add(col)))
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

    const rowsToUpdate = rows.filter(r => !!rowById[r.id])
    const rowsToAppend = rows.filter(r => !rowById[r.id])
    // console.log({
    //   dbmsToUpdate: dbmsToUpdate.length,
    //   dbmsToAppend: dbmsToAppend.length,
    // })

    if (rowsToUpdate.length) {
      await this.sheets().spreadsheets.batchUpdate({
        spreadsheetId: this.cfg.spreadsheetId,

        requestBody: {
          requests: rowsToUpdate.map(r => ({
            updateCells: {
              fields: '*',
              start: {
                rowIndex: rowById[r.id],
                columnIndex: 1,
                sheetId,
              },
              rows: [
                {
                  values: cols.slice(1).map(col => this.asCellData(r[col])),
                },
              ],
            },
          })),
        },
      })
    }

    if (rowsToAppend.length) {
      await this.sheets().spreadsheets.values.append({
        spreadsheetId: this.cfg.spreadsheetId,
        range: `${table}!A${nextRow}`,
        valueInputOption: 'RAW',
        requestBody: {
          values: rowsToAppend.map(r => cols.map(col => r[col])),
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
    const { rows } = await this.runQuery(q.select(['id']), opt)
    const ids = rows.map(r => r.id)

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

  async runQuery<ROW extends ObjectWithId, OUT = ROW>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    const rows = await this.getAllRows<ROW>(q.table)

    return {
      rows: queryInMemory<ROW, OUT>(q, rows),
    }
  }

  async runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    const { rows } = await this.runQuery(q, opt)
    return rows.length
  }

  streamQuery<ROW extends ObjectWithId, OUT = ROW>(
    q: DBQuery<ROW>,
    opt?: CommonDBStreamOptions,
  ): ReadableTyped<OUT> {
    const readable = readableCreate<ROW>()

    void this.runQuery(q, opt).then(({ rows }) => {
      rows.forEach(r => readable.push(r))
      readable.push(null) // done
    })

    return readable
  }

  async getTableSchema<ROW extends ObjectWithId>(table: string): Promise<CommonSchema<ROW>> {
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

    const rows = (res.data.sheets || []).map(s => s.properties!)
    return _by(rows, 'title')
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
}
