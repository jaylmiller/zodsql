import assert from 'assert';
import {CreateTableBuilder, Kysely} from 'kysely';
import {z} from 'zod';
import {ColumnData, ZsqlColumn} from './column';
import {NoDatabaseFound} from './errs';
import {
  expectType,
  objItems,
  processCreateParams,
  RawCreateParams
} from './util';

export type ZsqlRawShape = {
  [k: string]: ZsqlColumn<any, any, any>;
};

expectType<ZsqlRawShape extends z.ZodRawShape ? true : false>(true);
type UnknownKeysParam = 'passthrough' | 'strict' | 'strip';

export type TableParams = {
  name: string;
  // arbitrary options for custom use cases
  // could be used to apply perms or other after table creation
  extraOpts?: any;
};

/**
 * generates the name of a primary key for a given table when generating it's ddl.
 * can be overriden
 * @param table
 * @param pks
 * @returns
 */
const defaultPkNameGen = (
  table: string,
  pks: Array<ColumnData & {name: string}>
) => `pk_${table}_${pks.map(c => c.name).join('_')}`;
export type PrimaryKeyNameFn = typeof defaultPkNameGen;
export class ZsqlTable<
  T extends ZsqlRawShape,
  UnknownKeys extends UnknownKeysParam = 'strip',
  Catchall extends z.ZodTypeAny = z.ZodTypeAny,
  O = z.objectOutputType<T, Catchall>,
  I = z.objectInputType<T, Catchall>
  // a table should have less flexibility than the ZodObject so we extend the root type
  // and just use the parser from ZodObject
> extends z.ZodType<O, z.ZodObjectDef<T, UnknownKeys, Catchall>, I> {
  name!: string;
  columns!: T;
  private db?: Kysely<any>;
  // column data
  private __cols!: Array<ColumnData & {name: string}>;
  // only set by ZsqlSchema
  __schema?: string;
  /**
   * get create table DDL
   */
  ddl(opts?: {pkNameFn: PrimaryKeyNameFn}) {
    if (!this.db) throw new NoDatabaseFound();

    const pkNameFn = opts?.pkNameFn || defaultPkNameGen;
    const pks = this.__cols.filter(c => c.primaryKey);
    let stmt: CreateTableBuilder<string, string> = (
      this.__schema ? this.db.schema.withSchema(this.__schema) : this.db.schema
    ).createTable(this.name);
    for (let coldef of this.__cols) {
      stmt = stmt.addColumn(coldef.name, coldef.dataType, col => {
        if (coldef.required) col = col.notNull();
        if (coldef.defaultVal) col = col.defaultTo(coldef.defaultVal);
        return col;
      });
    }
    if (pks.length > 0)
      stmt = stmt.addPrimaryKeyConstraint(
        pkNameFn(this.name, pks),
        pks.map(c => c.name)
      );
    return stmt;
  }

  private _getQueryBuilder() {
    if (!this.db) throw new NoDatabaseFound();
    return this.__schema ? this.db.withSchema(this.__schema) : this.db;
  }

  /**
   * generate an insert statement from data
   */
  insert(d: O | Array<O>) {
    const db = this._getQueryBuilder();
    const parsed = Array.isArray(d)
      ? d.map(p => this.parse(p))
      : [this.parse(d)];
    return db.insertInto(this.name).values(parsed);
  }

  /**
   * Binds the database api object to the table
   * @param db
   */
  bindDb(db: Kysely<any>) {
    this.db = db;
    return this;
  }

  /**
   * Avoid using this outside of tests. Intended use is via the schema objects.
   * @returns
   */
  getDb(): Kysely<any> {
    if (!this.db) throw new NoDatabaseFound();
    return this.db;
  }

  // need this so that the zod object parser will work
  private _cached: {shape: T; keys: string[]} | null = null;
  _parse(input: z.ParseInput): z.ParseReturnType<this['_output']> {
    return z.ZodObject.prototype._parse.call(this, input);
  }
  _getCached(): {shape: T; keys: string[]} {
    return z.ZodObject.prototype._getCached.call(this);
  }
  static create<T extends ZsqlRawShape>(
    tableParams: (TableParams & {db?: Kysely<any>}) | string,
    shape: T,
    params?: RawCreateParams
  ): ZsqlTable<T> {
    const {name, extraOpts, db} =
      typeof tableParams === 'object'
        ? tableParams
        : {name: tableParams, db: undefined, extraOpts: undefined};

    const newInst = new ZsqlTable({
      shape: () => shape,
      unknownKeys: 'strip',
      catchall: z.ZodNever.create(),
      typeName: z.ZodFirstPartyTypeKind.ZodObject,
      ...processCreateParams(params)
    });
    newInst.columns = shape;
    newInst.__cols = objItems(shape).map(([k, v]) => {
      assert(typeof k === 'string');
      const coldata = v._getColData();
      return {...coldata, name: k};
    });
    newInst._cached = null;
    newInst.name = name;
    newInst.db = db;
    return newInst;
  }
  /**
   * Returns an fn that behaves like z.object but has the db api attached
   * @param db
   * @returns
   */
  static getNewTableFn(db: Kysely<any>): typeof ZsqlTable.create {
    const fn = ZsqlTable.create;
    return function (...args) {
      const newInst = fn(...args);
      newInst.db = db;
      return newInst;
    } as typeof ZsqlTable.create;
  }

  /**
   * return the sql data for this class (i.e. the stuff required to generate DDL )
   */
  _getSqlData() {
    return {
      name: this.name,
      cols: this.__cols
    };
  }
}

export const table = ZsqlTable.create;
export const createTableFn = ZsqlTable.getNewTableFn;
