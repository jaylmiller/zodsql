// a schema/namespace that can be queried over

import assert from 'assert';
import { objItems } from '../util';
import { ZsqlRawShape, ZsqlTable } from './table';

type _InferRaw<T extends { [k: string]: ZsqlTable<ZsqlRawShape> }> = {
  [K in keyof T]: T[K]['_output'];
};

export type InferSchema<T extends ZsqlSchema<AnySchema>> = _InferRaw<
  T['tables']
>;

type AnySchema = { [table: string]: ZsqlTable<ZsqlRawShape> };

export class ZsqlSchema<T extends AnySchema> {
  tables: T;
  constructor(schema: T) {
    this.tables = schema;
  }

  zodObj<K extends keyof T>(k: K): T[K] {
    return this.tables[k];
  }

  static create<Schema extends { [table: string]: ZsqlRawShape }>(
    schemaObj: Schema,
    name?: string
  ): ZsqlSchema<{
    [K in keyof Schema]: ZsqlTable<Schema[K]>;
  }> {
    return new ZsqlSchema(
      objItems(schemaObj).reduce(
        (acc, [tableName, tableCols]) => ({
          ...acc,
          [tableName]: ZsqlTable.create(
            typeof tableName === 'string'
              ? { name: tableName, schema: name }
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
