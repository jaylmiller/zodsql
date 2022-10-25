import assert from 'assert';
import {ColumnDataType, sql} from 'kysely';
import {any, z, ZodError} from 'zod';
import {
  addIssueToContext,
  INVALID,
  processCreateParams,
  RawCreateParams
} from './util';

export type ColumnData = {
  dataType: ColumnDataType;
  required?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  // default calculated server-side: pass in raw sql value with the template literal
  defaultVal?: ReturnType<typeof sql>;
};

type ClassConstructor<T> = {
  new (...args: any[]): T;
};

export abstract class ZsqlColumn<
  Output = any,
  Def extends z.ZodTypeDef = z.ZodTypeDef,
  Input = Output,
  IsPk extends boolean | undefined = false
  // ColData extends Omit<ColumnData, 'dataType'> =
> extends z.ZodType<Output, Def, Input> {
  // zod types are non-optional by default
  private coldata: Omit<ColumnData, 'dataType'> = {required: true};
  __isPk!: IsPk;
  // optional type identifier
  _id?: string;
  sqlType!: ColumnDataType;
  zodType!: ClassConstructor<z.ZodType<Output, Def, Input>>;
  _columnData(): ColumnData {
    assert(this.sqlType);
    return {
      ...this.coldata,
      dataType: this.sqlType
    };
  }
  // abstract toZodType(): z.ZodType<Output, Def, Input> | null;
  optional(): ZsqlColumnOptional<this> {
    if (this.coldata.primaryKey)
      throw new Error('PK columns cannot be optional');
    this.coldata.required = false;
    const newCol = ZsqlColumnOptional.create(this);
    newCol.coldata = this.coldata;
    newCol.sqlType = this.sqlType;
    newCol._id = this._id;
    return newCol;
  }

  /**
   * runs the base zod parser. e.g. for ZsqlString run the ZodString _parse
   */
  _zodParser(input: z.ParseInput): z.ParseReturnType<Output> {
    return this.zodType.prototype._parse.call(this, input);
  }

  /**
   * creates a new instance of this class with the same data
   * @returns
   */
  copy(): this {
    // zod internal data is storede in _def
    const newInst = new (Object.getPrototypeOf(this).constructor)({
      ...this._def
    });
    // copy our extra data to new object
    newInst.sqlType = this.sqlType;
    newInst.coldata = {...this.coldata};
    newInst._id = this._id;
    return newInst;
  }

  serverDefault(rawSql: ReturnType<typeof sql>) {
    const newobj = this.copy();
    newobj.coldata.defaultVal = rawSql;
    return newobj;
  }

  unique() {
    const newobj = this.copy();
    newobj.coldata.unique = true;
    return newobj;
  }

  abstract toZodType(): z.ZodType<Output, Def, Input>;

  primaryKey(): ZsqlColumn<Output, Def, Input, true> {
    const newobj = this.copy() as ZsqlColumn<Output, Def, Input, true>;
    newobj.coldata.primaryKey = true;
    return newobj;
  }
}
type ZsqlColumnAny = ZsqlColumn<any, any, any, any>;
export class ZsqlColumnOptional<T extends ZsqlColumnAny> extends ZsqlColumn<
  T['_output'] | undefined,
  z.ZodOptionalDef<T>,
  T['_input']
> {
  _parse(input: z.ParseInput): z.ParseReturnType<this['_output']> {
    const parsedType = this._getType(input);
    // treat null and undefined the same since SQL does
    if (
      parsedType === z.ZodParsedType.undefined ||
      parsedType === z.ZodParsedType.null
    ) {
      return {status: 'valid', value: undefined};
    }
    return this._def.innerType._parse(input);
  }
  unwrap() {
    return this._def.innerType;
  }
  static create<T_1 extends ZsqlColumnAny>(
    type: T_1,
    params?: RawCreateParams
  ) {
    return new ZsqlColumnOptional({
      innerType: type,
      typeName: z.ZodFirstPartyTypeKind.ZodOptional,
      ...processCreateParams(params)
    });
  }

  toZodType(): z.ZodType<
    T['_output'] | undefined,
    z.ZodOptionalDef<T>,
    T['_input']
  > {
    // TODO: figure out typing
    return z.ZodOptional.create(this._def.innerType.toZodType(), {
      ...this._def
    }) as unknown as any;
    // const inner = this._def.innerType;
    // if (
    //   inner instanceof ZsqlString || inner instanceof ZsqlInt
    //   // inner instanceof ZsqlTimestamp
    // ) {
    //   return z.ZodOptional.create(inner, {...this._def});
    // }
    // if (inner instanceof ZsqlInt)
    //   return z.ZodOptional.create(inner, {...this._def});
    // throw new Error('unexpected');
  }
}

export class ZsqlString extends ZsqlColumn<string, z.ZodStringDef> {
  sqlType: ColumnDataType = 'text';
  zodType = z.ZodString;

  _parse(input: z.ParseInput): z.ParseReturnType<this['_output']> {
    return this._zodParser(input);
  }

  static create(params?: RawCreateParams) {
    return new ZsqlString({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodString,
      ...processCreateParams(params)
    } as z.ZodStringDef);
  }

  toZodType(): z.ZodType<string, z.ZodStringDef, string> {
    return z.ZodString.create({...this._def});
  }
}

export class ZsqlInt extends ZsqlColumn<number, z.ZodNumberDef> {
  sqlType: ColumnDataType = 'integer';
  zodType = z.ZodNumber;
  _parse(input: z.ParseInput): z.ParseReturnType<this['_output']> {
    // custom parse
    if (!Number.isInteger(input.data)) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        code: z.ZodIssueCode.invalid_type,
        expected: z.ZodParsedType.integer,
        received: ctx.parsedType
      });
      return INVALID;
    }
    // then use zods parser
    return this._zodParser(input);
  }

  static create(params?: RawCreateParams) {
    return new ZsqlInt({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodNumber,
      ...processCreateParams(params)
    } as z.ZodNumberDef);
  }

  toZodType(): z.ZodType<number, z.ZodNumberDef, number> {
    return z.ZodNumber.create({...this._def});
  }
}

// TODO: not sure how to differentiate dates and timestamps client side
export class ZsqlTimestamp extends ZsqlColumn<Date, z.ZodDateDef> {
  sqlType: ColumnDataType = 'timestamp';
  zodType = z.ZodDate;
  _parse(input: z.ParseInput): z.ParseReturnType<this['_output']> {
    return this._zodParser(input);
  }
  static create(params?: RawCreateParams) {
    return new ZsqlTimestamp({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodDate,
      ...processCreateParams(params)
    } as z.ZodDateDef);
  }

  toZodType(): z.ZodType<Date, z.ZodDateDef, Date> {
    return z.ZodDate.create({...this._def});
  }
}
export class ZsqlDate extends ZsqlColumn<Date, z.ZodDateDef> {
  sqlType: ColumnDataType = 'date';
  zodType = z.ZodDate;
  _parse(input: z.ParseInput): z.ParseReturnType<any> {
    return this._zodParser(input);
  }
  static create(params?: RawCreateParams) {
    return new ZsqlDate({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodDate,
      ...processCreateParams(params)
    } as z.ZodDateDef);
  }
  toZodType(): z.ZodType<Date, z.ZodDateDef, Date> {
    return z.ZodDate.create({...this._def});
  }
}
export class ZsqlBool extends ZsqlColumn<boolean, z.ZodBooleanDef> {
  sqlType: ColumnDataType = 'boolean';
  zodType = z.ZodBoolean;
  _parse(input: z.ParseInput): z.ParseReturnType<any> {
    return this._zodParser(input);
  }
  static create(params?: RawCreateParams) {
    return new ZsqlBool({
      typeName: z.ZodFirstPartyTypeKind.ZodBoolean,
      ...processCreateParams(params)
    } as z.ZodBooleanDef);
  }
  toZodType(): z.ZodType<boolean, z.ZodBooleanDef, boolean> {
    return z.ZodBoolean.create({...this._def});
  }
}

export class ZsqlNumeric extends ZsqlColumn<number, z.ZodNumberDef> {
  sqlType: ColumnDataType = 'numeric';
  zodType = z.ZodNumber;
  _parse(input: z.ParseInput): z.ParseReturnType<this['_output']> {
    // not sure if we even need this but feel like it should do some sort
    // of client validation here lol
    if (!Number.isFinite(input.data)) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        code: z.ZodIssueCode.invalid_type,
        expected: z.ZodParsedType.float,
        received: ctx.parsedType
      });
      return INVALID;
    }
    // zod parser
    return this._zodParser(input);
  }

  static create(params?: RawCreateParams) {
    return new ZsqlNumeric({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodNumber,
      ...processCreateParams(params)
    } as z.ZodNumberDef);
  }

  toZodType(): z.ZodType<number, z.ZodNumberDef, number> {
    return z.ZodNumber.create({...this._def});
  }
}

export class ZsqlBigInt extends ZsqlColumn<BigInt, z.ZodBigIntDef> {
  sqlType: ColumnDataType = 'bigint';
  zodType = z.ZodBigInt;
  _parse(input: z.ParseInput): z.ParseReturnType<this['_output']> {
    return this._zodParser(input);
  }

  static create(params?: RawCreateParams) {
    return new ZsqlBigInt({
      typeName: z.ZodFirstPartyTypeKind.ZodBigInt,
      ...processCreateParams(params)
    } as z.ZodBigIntDef);
  }

  toZodType(): z.ZodType<BigInt, z.ZodBigIntDef, BigInt> {
    return z.ZodBigInt.create({...this._def});
  }
}

export class ZsqlBin extends ZsqlColumn<Buffer, z.ZodAnyDef> {
  sqlType: ColumnDataType = 'binary';
  zodType = z.ZodAny;
  _parse(input: z.ParseInput): z.ParseReturnType<this['_output']> {
    if (!(input.data instanceof Buffer)) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        message: 'expected data to be instanceof Buffer',
        code: z.ZodIssueCode.invalid_type,
        // TODO: not sure what this should be
        expected: z.ZodParsedType.unknown,
        received: ctx.parsedType
      });
      return INVALID;
    }
    return {
      status: 'valid',
      value: input.data
    };
  }

  static create(params?: RawCreateParams) {
    return new ZsqlBin({
      typeName: z.ZodFirstPartyTypeKind.ZodAny,
      ...processCreateParams(params)
    } as z.ZodAnyDef);
  }

  toZodType(): z.ZodType<Buffer, z.ZodAnyDef, Buffer> {
    return z.ZodAny.create({...this._def});
  }
}

export class ZsqlJsonb extends ZsqlColumn<any, any> {
  // TODO: make this take objects too
  sqlType: ColumnDataType = 'jsonb';
  zodType = z.ZodAny;
  _parse(input: z.ParseInput) {
    const parsedType = this._getType(input);
    if (parsedType === 'object') {
      return this._zodParser(input);
    }
    try {
      JSON.parse(input.data);
    } catch (e) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        code: z.ZodIssueCode.custom,
        message: 'String is invalid JSON'
      });
      return INVALID;
    }
    return this._zodParser(input);
  }
  toZodType(): z.ZodType<any, any, any> {
    return z.ZodAny.create({...this._def});
  }
  static create(params?: RawCreateParams) {
    return new ZsqlJsonb({
      typeName: z.ZodFirstPartyTypeKind.ZodAny,
      ...processCreateParams(params)
    } as z.ZodAnyDef);
  }
}
/**
 * Catch-all  column with no validation that takes any sqlType as input.
 */
export class ZsqlCustomColumn<T = any> extends ZsqlColumn<T> {
  zodType = z.ZodAny;
  _id?: string;
  _parse(input: z.ParseInput) {
    return this._zodParser(input);
  }
  toZodType() {
    return z.ZodAny.create({...this._def});
  }
  static create<T_1>(
    params: RawCreateParams & {sqlType: ColumnDataType; id?: string}
  ) {
    const col = new ZsqlCustomColumn<T_1>({
      typeName: z.ZodFirstPartyTypeKind.ZodAny,
      ...processCreateParams(params)
    } as z.ZodAnyDef);
    col.sqlType = params.sqlType;
    col._id = params.id;
    return col;
  }
}

export const columns = {
  text: ZsqlString.create,
  int: ZsqlInt.create,
  numeric: ZsqlNumeric.create,
  date: ZsqlDate.create,
  timestamp: ZsqlTimestamp.create,
  binary: ZsqlBin.create,
  bigint: ZsqlBigInt.create,
  bool: ZsqlBool.create,
  jsonb: ZsqlJsonb.create,
  custom: ZsqlCustomColumn.create
};

const innerTypeMap = {};
