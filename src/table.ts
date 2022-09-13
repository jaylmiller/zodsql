import assert from 'assert';
import {CreateTableBuilder, Kysely, Transaction} from 'kysely';
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
  [k: string]: ZsqlColumn<any, any, any, any>;
};

expectType<ZsqlRawShape extends z.ZodRawShape ? true : false>(true);
type UnknownKeysParam = 'passthrough' | 'strict' | 'strip';

export type TableParams<OptsType extends object = {}> = {
  name: string;
  // arbitrary options for custom use cases
  // could be used to apply perms or other after table creation
  extraOpts?: OptsType;
};
// export type PrimaryKeys<T extends ZsqlRawShape> = {
//   [K in keyof T]: T[K]['__isPk'] extends true ? T[K] : never;
// };
type PrimaryKeyRaw<T extends ZsqlRawShape> = {
  [K in keyof T as T[K]['__isPk'] extends true ? K : never]: T[K]['_output'];
};
export type PrimaryKey<T extends ZsqlTable<any, any>> = PrimaryKeyRaw<
  T['columns']
>;
// export type PrimaryKey2<T extends ZsqlTable<any, any>> = {
//   [K in keyof T['columns'] as T['columns'][K]['__isPk'] extends true
//     ? K
//     : never]: T[K][];
// };
type PkObj<T extends ZsqlRawShape> = {
  [K in keyof T as T[K]['__isPk'] extends true ? K : never]: T[K]['_output'];
};
export type ZsqlShapeToZod<T extends ZsqlRawShape> = {
  [K in keyof T]: z.ZodType<T[K]['_output'], T[K]['_def'], T[K]['_input']>;
};
export type InferZsqlRawShape<T extends ZsqlRawShape> = {
  [K in keyof T]: T[K]['_output'];
};
export class ZsqlTable<
  T extends ZsqlRawShape,
  CustomOpts extends object = {},
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

  customData?: CustomOpts;
  /**
   * get create table DDL
   */
  ddl(opts?: {
    pkNameFn?: PrimaryKeyNameFn;
    fks?: {
      col: string;
      to: {table: string; cols: string};
    };
    noPrimaryKeys?: boolean;
  }) {
    if (!this.db) throw new NoDatabaseFound();
    const skipPk = !!opts?.noPrimaryKeys;
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
    if (pks.length > 0 && !skipPk)
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
   * Binds a database api object to the table
   * @param db
   */
  bindDb(db: Kysely<any> | Transaction<any>) {
    this.db = db;
    return this;
  }

  get pk(): PrimaryKeyRaw<T> {
    return objItems(this.columns).reduce(
      (acc, [k, v]) => ({
        ...acc,
        ...(v._columnData().primaryKey ? {[k]: v} : {})
      }),
      {} as PrimaryKeyRaw<T>
    );
  }

  async fromPk(pk: PkObj<T>) {
    let stmt = this._getQueryBuilder().selectFrom(this.name).selectAll();
    objItems(pk).forEach(([k, v]) => {
      assert(typeof k === 'string');
      stmt = stmt.where(k, '=', v);
    });
    const res = await stmt.executeTakeFirst();
    return res as unknown as {[K in keyof T]: T[K]['_output']} | undefined;
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
    if (typeof this._cached === 'undefined') this._cached = null;
    return z.ZodObject.prototype._parse.call(this, input);
  }
  _getCached(): {shape: T; keys: string[]} {
    return z.ZodObject.prototype._getCached.call(this);
  }

  toZodShape(): ZsqlShapeToZod<T> {
    return objItems(this.columns).reduce(
      (acc, [name, col]) => ({
        ...acc,
        [name]: col.toZodType()
      }),
      {} as ZsqlShapeToZod<T>
    );
  }
  toZodType(): z.ZodObject<ZsqlShapeToZod<T>> {
    return z.ZodObject.create(this.toZodShape());
  }

  static create<T extends ZsqlRawShape, CustomOpts extends object = {}>(
    tableParams: (TableParams<CustomOpts> & {db?: Kysely<any>}) | string,
    shape: T,
    params?: RawCreateParams
  ): ZsqlTable<T> {
    const {name, extraOpts, db} =
      typeof tableParams === 'object'
        ? tableParams
        : {name: tableParams, db: undefined, extraOpts: undefined};

    const newInst = new ZsqlTable<T, CustomOpts>({
      shape: () => shape,
      unknownKeys: 'strip',
      catchall: z.ZodNever.create(),
      typeName: z.ZodFirstPartyTypeKind.ZodObject,
      ...processCreateParams(params)
    });
    newInst.customData = extraOpts;
    newInst.columns = shape;
    newInst.__cols = objItems(shape).map(([k, v]) => {
      assert(typeof k === 'string');
      const coldata = v._columnData();
      return {...coldata, name: k};
    });
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

  _copyShape() {
    return objItems(this.columns).reduce(
      (acc, [k, v]) => ({...acc, [k]: v.copy()}),
      {} as T
    );
  }

  /**
   * attach new custom data to the table
   */
  _withCustomData<NewCust extends object>(
    newData: NewCust
  ): ZsqlTable<T, NewCust, UnknownKeys, Catchall, O, I> {
    const cur = this as unknown as ZsqlTable<
      T,
      NewCust,
      UnknownKeys,
      Catchall,
      O,
      I
    >;
    cur.customData = newData;
    return cur;
  }
  // deepCopy() {
  //   const cols = objItems(this.columns).reduce(
  //     (acc, [k, v]) => ({...acc, [k]: v.copy()}),
  //     {} as T
  //   );
  //   return ZsqlTable.create<T>({name: this.name, db: this.db}, cols);
  // }
}

export const table = ZsqlTable.create;
export const createTableFn = ZsqlTable.getNewTableFn;
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
