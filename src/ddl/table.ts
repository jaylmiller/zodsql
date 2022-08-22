import assert from 'assert';
import { z, ZodObject } from 'zod';
import {
  expectType,
  objItems,
  processCreateParams,
  RawCreateParams
} from '../util';
import { ZsqlAny, ZsqlColumn, ColumnData } from './column';

export type ZsqlRawShape = {
  [k: string]: ZsqlColumn<any, any, any>;
};

expectType<ZsqlRawShape extends z.ZodRawShape ? true : false>(true);
type UnknownKeysParam = 'passthrough' | 'strict' | 'strip';

export type TableParams = {
  name: string;
  // namespace
  schema?: string;
};

export class ZsqlTable<
  T extends ZsqlRawShape,
  UnknownKeys extends UnknownKeysParam = 'strip',
  Catchall extends z.ZodTypeAny = z.ZodTypeAny,
  Output = z.objectOutputType<T, Catchall>,
  Input = z.objectInputType<T, Catchall>
  // a table should ahve less flexibility than the ZodObject so we extend the root type
  // and just use the parser from ZodObject
> extends z.ZodType<Output, z.ZodObjectDef<T, UnknownKeys, Catchall>, Input> {
  private __table!: TableParams;
  private __cols!: Array<ColumnData & { name: string }>;
  // need this so that the zod object parser will work
  private _cached: { shape: T; keys: string[] } | null = null;
  _parse(input: z.ParseInput): z.ParseReturnType<this['_output']> {
    return z.ZodObject.prototype._parse.call(this, input);
  }
  _getCached(): { shape: T; keys: string[] } {
    return z.ZodObject.prototype._getCached.call(this);
  }
  static create<T extends ZsqlRawShape>(
    tableParams: TableParams | string,
    shape: T,
    params?: RawCreateParams
  ): ZsqlTable<T> {
    const { name, schema } =
      typeof tableParams === 'string'
        ? { name: tableParams, schema: undefined }
        : tableParams;
    const newobj = new ZsqlTable({
      shape: () => shape,
      unknownKeys: 'strip',
      catchall: z.ZodNever.create(),
      typeName: z.ZodFirstPartyTypeKind.ZodObject,
      ...processCreateParams(params)
    });
    newobj.__cols = objItems(shape).map(([k, v]) => {
      assert(typeof k === 'string');
      const coldata = v._getColData();
      return { ...coldata, name: k };
    });
    newobj._cached = null;
    newobj.__table = { name, schema };
    // give the zod object funcs but dont expose them to the typings api
    return newobj;
  }

  /**
   * return the sql data for this class (i.e. the stuff required to generate DDL )
   */
  _getSqlData() {
    return {
      ...this.__table,
      cols: this.__cols
    };
  }
}

export const table = ZsqlTable.create;
