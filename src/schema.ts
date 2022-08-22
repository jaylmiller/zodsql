// a schema/namespace that can be queried over

import assert from 'assert';
import {objItems} from './util';
import {ZsqlRawShape, ZsqlTable} from './table';
import {Kysely} from 'kysely';
import {NoDatabaseFound} from './errs';

type _InferRaw<T extends {[k: string]: ZsqlTable<ZsqlRawShape>}> = {
  [K in keyof T]: T[K]['_output'];
};

export type InferSchema<T extends ZsqlSchema<AnySchema>> = _InferRaw<
  T['tables']
>;

type AnySchema = {[table: string]: ZsqlTable<ZsqlRawShape>};

type SchemaParams = {
  db?: Kysely<any>;
  name?: string;
};

export class ZsqlSchema<T extends AnySchema> {
  tables: T;
  private db?: Kysely<any>;
  _name?: string;
  constructor(schema: T) {
    this.tables = schema;
  }

  bindDb(db: Kysely<any>) {
    this.db = db;
    // bind db to tables in this schema
    objItems(this.tables).forEach(([_, table]) => {
      table.bindDb(db);
    });
    return this;
  }

  getDb() {
    if (!this.db) throw new NoDatabaseFound();
    const tables = this.tables;
    type T1 = _InferRaw<typeof tables>;
    return this.db as Kysely<T1>;
  }

  /**
   * regular create functionality (works like zod.object)
   */
  static create<Schema extends {[table: string]: ZsqlRawShape}>(
    schemaObj: Schema,
    params?: SchemaParams
  ): ZsqlSchema<{
    [K in keyof Schema]: ZsqlTable<Schema[K]>;
  }> {
    const {name, db} = params || {};
    const schema = ZsqlSchema.createHelper(schemaObj);
    schema._name = name;
    schema.db = db;
    return schema;
  }

  /**
   * Returns an fn that behaves like z.object but has the db api attached
   * @param db
   * @returns
   */
  static getSchemaDefFn(db: Kysely<any>): typeof ZsqlSchema.createHelper {
    const fn = ZsqlSchema.createHelper;
    return function (...args) {
      const newInst = fn(...args);
      newInst.db = db;
      return newInst;
    } as typeof ZsqlSchema.createHelper;
  }

  private static createHelper<Schema extends {[table: string]: ZsqlRawShape}>(
    schemaObj: Schema
  ): ZsqlSchema<{
    [K in keyof Schema]: ZsqlTable<Schema[K]>;
  }> {
    return new ZsqlSchema(
      objItems(schemaObj).reduce(
        (acc, [tableName, tableCols]) => ({
          ...acc,
          [tableName]: ZsqlTable.create(
            typeof tableName === 'string'
              ? tableName
              : () => {
                  throw new Error('Table names must be strings');
                },
            tableCols
          )
        }),
        {} as {
          [TableName in keyof Schema]: ZsqlTable<Schema[TableName]>;
        }
      )
    );
  }
}
