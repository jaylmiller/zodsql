import assert from 'assert';
import {ColumnDataType, sql} from 'kysely';
import {z} from 'zod';
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
  Input = Output
  // ColData extends Omit<ColumnData, 'dataType'> =
> extends z.ZodType<Output, Def, Input> {
  // zod types are non-optional by default
  protected __colData: Omit<ColumnData, 'dataType'> = {required: true};
  sqlType!: ColumnDataType;
  zodType!: ClassConstructor<z.ZodType<Output, Def, Input>>;
  _getColData(): ColumnData {
    assert(this.sqlType);
    return {
      ...this.__colData,
      dataType: this.sqlType
    };
  }
  optional(): ZsqlColumnOptional<this> {
    if (this.__colData.primaryKey)
      throw new Error('PK columns cannot be optional');
    this.__colData.required = false;
    const newCol = ZsqlColumnOptional.create(this);
    newCol.__colData = this.__colData;
    newCol.sqlType = this.sqlType;
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
  copySelf(): this {
    // zod internal data is storede in _def
    const newInst = new (Object.getPrototypeOf(this).constructor)({
      ...this._def
    });
    // copy our extra data to new object
    newInst.sqlType = this.sqlType;
    newInst.__colData = {...this.__colData};
    return newInst;
  }

  serverDefault(rawSql: ReturnType<typeof sql>) {
    const newobj = this.copySelf();
    newobj.__colData.defaultVal = rawSql;
    return newobj;
  }

  unique() {
    const newobj = this.copySelf();
    newobj.__colData.unique = true;
    return newobj;
  }

  primaryKey() {
    const newobj = this.copySelf();
    newobj.__colData.primaryKey = true;
    return newobj;
  }
}

export class ZsqlColumnOptional<T extends z.ZodTypeAny> extends ZsqlColumn<
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
  unwrap(): T {
    return this._def.innerType;
  }
  static create<T_1 extends z.ZodTypeAny>(type: T_1, params?: RawCreateParams) {
    return new ZsqlColumnOptional({
      innerType: type,
      typeName: z.ZodFirstPartyTypeKind.ZodOptional,
      ...processCreateParams(params)
    });
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
}
export const columns = {
  text: ZsqlString.create,
  int: ZsqlInt.create,
  numeric: ZsqlNumeric.create,
  date: ZsqlDate.create,
  timestamp: ZsqlTimestamp.create,
  binary: ZsqlBin.create,
  bigint: ZsqlBigInt.create,
  bool: ZsqlBool.create
};
