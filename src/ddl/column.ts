import assert from 'assert';
import { ColumnDataType, sql } from 'kysely';
import { any, z, ZodType } from 'zod';
import {
  addProp,
  processCreateParams,
  INVALID,
  addIssueToContext,
  objItems
} from '../util';

export interface ColumnDef {
  dataType: ColumnDataType;
  required?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  // default calculated server-side: pass in raw sql value with the template literal
  defaultVal?: ReturnType<typeof sql>;
}

export abstract class ZsqlColumn<
  Output = any,
  Def extends z.ZodTypeDef = z.ZodTypeDef,
  Input = Output
> extends z.ZodType<Output, Def, Input> {
  protected __colData: Omit<ColumnDef, 'dataType'> = { required: true };
  sqlType!: ColumnDataType;

  optional(): ZsqlColumnOptional<this> {
    if (this.__colData.primaryKey)
      throw new Error('PK columns cannot be optional');
    this.__colData.required = false;
    const newCol = ZsqlColumnOptional.create(this);
    newCol.__colData = this.__colData;
    newCol.sqlType = this.sqlType;
    return newCol;
  }

  private copySelf(): this {
    // zod internal data is storede in _def
    const newInst = new (Object.getPrototypeOf(this).constructor)({
      ...this._def
    });
    // copy our extra data to new object
    newInst.sqlType = this.sqlType;
    newInst.__colData = { ...this.__colData };
    return newInst;
  }

  _getColData(): ColumnDef {
    assert(this.sqlType);
    return {
      ...this.__colData,
      dataType: this.sqlType
    };
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
      return { status: 'valid', value: undefined };
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

type RawCreateParams =
  | {
      errorMap?: z.ZodErrorMap;
      invalid_type_error?: string;
      required_error?: string;
      description?: string;
    }
  | undefined;

export class ZsqlString extends ZsqlColumn<string, z.ZodStringDef> {
  sqlType: ColumnDataType = 'text';
  _parse(input: z.ParseInput): z.ParseReturnType<any> {
    const t = this;
    return z.ZodString.prototype._parse.call(this, input);
  }
  static create(params?: RawCreateParams) {
    return new ZsqlString({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodString,
      ...processCreateParams(params)
    } as z.ZodStringDef);
  }
}

class ZsqlInt extends ZsqlColumn<number, z.ZodNumberDef> {
  sqlType: ColumnDataType = 'integer';
  _parse(input: z.ParseInput): z.ParseReturnType<any> {
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
    return z.ZodNumber.prototype._parse.call(this, input);
  }

  static create(params?: RawCreateParams) {
    return new ZsqlInt({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodNumber,
      ...processCreateParams(params)
    } as z.ZodNumberDef);
  }
}

class ZsqlDateObj extends ZsqlColumn<Date, z.ZodDateDef> {
  _parse(input: z.ParseInput): z.ParseReturnType<any> {
    // custom parse
    return z.ZodDate.prototype._parse.call(this, input);
  }
  static create(params?: RawCreateParams) {
    return new ZsqlDateObj({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodDate,
      ...processCreateParams(params)
    } as z.ZodDateDef);
  }
}
// TODO: not sure how to differentiate dates and timestamps client side
class ZsqlTimestamp extends ZsqlDateObj {
  sqlType: ColumnDataType = 'timestamp';
  static create(params?: RawCreateParams) {
    return new ZsqlTimestamp({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodDate,
      ...processCreateParams(params)
    } as z.ZodDateDef);
  }
}
class ZsqlDate extends ZsqlDateObj {
  sqlType: ColumnDataType = 'date';
  static create(params?: RawCreateParams) {
    return new ZsqlDate({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodDate,
      ...processCreateParams(params)
    } as z.ZodDateDef);
  }
}

class ZsqlNumeric extends ZsqlColumn<number, z.ZodNumberDef> {
  sqlType: ColumnDataType = 'numeric';
  _parse(input: z.ParseInput): z.ParseReturnType<any> {
    // custom parse
    if (!Number.isFinite(input.data)) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        code: z.ZodIssueCode.invalid_type,
        expected: z.ZodParsedType.integer,
        received: ctx.parsedType
      });
      return INVALID;
    }
    return z.ZodNumber.prototype._parse.call(this, input);
  }

  static create(params?: RawCreateParams) {
    return new ZsqlNumeric({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodNumber,
      ...processCreateParams(params)
    } as z.ZodNumberDef);
  }
}

class ZsqlBin extends ZsqlColumn<Buffer, z.ZodAnyDef> {
  sqlType: ColumnDataType = 'binary';
  _parse(input: z.ParseInput): z.ParseReturnType<any> {
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

export namespace Columns {
  export const text = ZsqlString.create;
  export const int = ZsqlInt.create;
  export const numeric = ZsqlNumeric.create;
  export const date = ZsqlDate.create;
  export const timestamp = ZsqlTimestamp.create;
  export const binary = ZsqlBin.create;
}
