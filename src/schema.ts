// a schema/namespace that can be queried over

import assert from 'assert';
import {objItems} from './util';
import {InferZsqlRawShape, ZsqlRawShape, ZsqlTable} from './table';
import {
  CreateTableBuilder,
  CreateSchemaBuilder,
  Kysely,
  Transaction
} from 'kysely';
import {NoDatabaseFound, TableNotFound} from './errs';

export type _InferSchemaRaw<T extends {[k: string]: ZsqlTable<ZsqlRawShape>}> =
  {
    [K in keyof T]: T[K]['_output'];
  };

export type InferSchema<T extends ZsqlSchema<AnySchema>> = _InferSchemaRaw<
  T['tables']
>;

type AnySchema = {[table: string]: ZsqlTable<ZsqlRawShape>};

type SchemaParams = {
  db?: Kysely<any>;
  name?: string;
};
export type CreateNamedSchemaFn = <
  Schema extends {[table: string]: ZsqlRawShape}
>(
  name: string | Schema,
  schemaObj?: Schema
) => ZsqlSchema<{
  [K in keyof Schema]: ZsqlTable<Schema[K]>;
}>;

export class ZsqlSchema<T extends AnySchema> {
  tables: T;
  private db?: Kysely<any>;
  _name?: string;
  constructor(schema: T) {
    this.tables = schema;
  }
  /**
   * Return object for building dml. this is the main way to interact
   * with the db
   */
  dmlBuilder() {
    if (!this.db) throw new NoDatabaseFound();
    const tables = this.tables;
    type T1 = _InferSchemaRaw<typeof tables>;
    if (this._name) return (this.db as Kysely<T1>).withSchema(this._name);
    return this.db as Kysely<T1>;
  }

  /**
   * Pass a callback in that lets you use this schema object within a transaction
   * @param runTx
   * @returns
   */
  async transaction(runTx: (schema: ZsqlSchema<T>) => Promise<any>) {
    if (!this.db) throw new NoDatabaseFound();
    return this.db.transaction().execute(async tx => {
      const schema = this._copy().bindDb(tx);
      return await runTx(schema);
    });
  }

  private _fks?: Fk<T>[];

  addForeignKey<
    FromTable extends keyof _InferSchemaRaw<T>,
    ToTable extends Exclude<keyof _InferSchemaRaw<T>, FromTable>
  >(def: {
    from: {
      table: FromTable;
      column: keyof T[FromTable]['_output'];
    };
    to: {
      table: ToTable;
      column: keyof T[ToTable]['_output'];
    };
  }) {
    if (!this._fks) this._fks = [];
    this._fks.push(def);
  }

  _copy(): ZsqlSchema<T> {
    return ZsqlSchema.create(
      objItems(this.tables).reduce((acc, [name, table]) => {
        assert(typeof name === 'string');
        return {
          ...acc,
          [name]: table._copyShape()
        };
      }, {} as {[K in keyof T]: T[K]['_output']}),
      {db: this.db, name: this._name}
    ) as ZsqlSchema<T>;
  }

  /**
   * Bind the database object to this schema
   */
  bindDb(db: Kysely<any> | Transaction<any>) {
    this.db = db;
    // bind db to tables in this schema
    objItems(this.tables).forEach(([_, table]) => {
      table.bindDb(db);
    });
    return this;
  }

  /**
   * set/change the name of the schema
   * @param name
   */
  setName(name: string) {
    this._name = name;
    objItems(this.tables).forEach(([_, table]) => {
      table.__schema = name;
    });
    return this;
  }

  /**
   * get table object from schema or throw
   * @param name
   */
  getTable(name: string) {
    for (let [tname, t] of objItems(this.tables)) {
      if (name === tname) return t;
    }
    throw new TableNotFound(name);
  }

  /**
   * return the DDL for this entire schema
   */
  ddl() {
    if (!this.db) throw new NoDatabaseFound();
    const out = [] as Array<CreateSchemaBuilder | CreateTableBuilder<any, any>>;
    if (this._name) {
      out.push(this.db.schema.createSchema(this._name).ifNotExists());
    }

    objItems(this.tables).forEach(([name, table]) => {
      const fks = this._fks?.filter(fk => fk.from.table === name) || [];
      out.push(table.ddl().ifNotExists());
    });
    return out;
  }

  /**
   * Return the full db object. Avoid using this one (use dmlBuilder)
   */
  _getDb() {
    if (!this.db) throw new NoDatabaseFound();
    const tables = this.tables;
    type T1 = _InferSchemaRaw<typeof tables>;
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
    if (db) schema.bindDb(db);
    if (name) schema.setName(name);
    return schema;
  }

  /**
   * Returns an fn that behaves like z.object but has the db api attached
   * @param db
   * @returns
   */
  static getNewSchemaFn(db: Kysely<any>): CreateNamedSchemaFn {
    const fn = ZsqlSchema.createHelper;
    return function (...args) {
      const newInst = fn(...args);
      newInst.db = db;
      objItems(newInst.tables).forEach(([_, v]) => {
        v.bindDb(db);
      });
      return newInst;
    } as CreateNamedSchemaFn;
  }

  private static createHelper<Schema extends {[table: string]: ZsqlRawShape}>(
    nameOrSchema: string | Schema,
    obj?: Schema
  ): ZsqlSchema<{
    [K in keyof Schema]: ZsqlTable<Schema[K]>;
  }> {
    const schemaObj = typeof nameOrSchema === 'string' ? obj : nameOrSchema;
    if (typeof schemaObj === 'undefined')
      throw new Error(`invalid args, must supply schema object`);
    const newinst = new ZsqlSchema(
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
    if (typeof nameOrSchema === 'string') {
      newinst._name = nameOrSchema;
      objItems(newinst.tables).forEach(([_, table]) => {
        table.__schema = nameOrSchema;
      });
    }
    return newinst;
  }
}
type __AllCols<T extends AnySchema> = {
  [K in keyof _InferSchemaRaw<T>]: keyof {
    [K2 in keyof _InferSchemaRaw<T>[K]]: null;
  };
}[keyof _InferSchemaRaw<T>];
type Fk<T extends AnySchema> = {
  from: {
    table: keyof _InferSchemaRaw<T>;
    column: __AllCols<T>;
  };
  to: {
    table: keyof _InferSchemaRaw<T>;
    column: __AllCols<T>;
  };
};
export const schema = ZsqlSchema.create;
export const createSchemaFn = ZsqlSchema.getNewSchemaFn;
