import { ColumnDataType, sql } from 'kysely';
import { any, z, ZodType } from 'zod';
import {
  addProp,
  processCreateParams,
  INVALID,
  addIssueToContext
} from '../util';

export interface ColumnDef {
  name: string;
  dataType: ColumnDataType;
  required?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  // default calculated server-side: pass in raw sql value with the template literal
  defaultVal?: ReturnType<typeof sql>;
}

abstract class ZodColumn<
  Output = any,
  Def extends z.ZodTypeDef = z.ZodTypeDef,
  Input = Output
> extends z.ZodType<Output, Def, Input> {
  // abstract class ZodColumn extends z.ZodType {
  _colData: Omit<ColumnDef, 'name' | 'dataType'> = { required: true };
  sqlType!: ColumnDataType;

  public optional(): ZodColumnOptional<this> {
    if (this._colData.primaryKey)
      throw new Error('PK columns cannot be optional');
    this._colData.required = false;
    return ZodColumnOptional.create(this);
  }

  public serverDefault(rawSql: ReturnType<typeof sql>) {
    this._colData.defaultVal = rawSql;
    return this;
  }

  public unique() {
    this._colData.unique = true;
    return this;
  }

  public primaryKey() {
    this._colData.primaryKey = true;
    return this;
  }
}

export class ZodColumnOptional<T extends z.ZodTypeAny> extends ZodColumn<
  T['_output'] | undefined,
  z.ZodOptionalDef<T>,
  T['_input']
> {
  _parse(input: z.ParseInput): z.ParseReturnType<this['_output']> {
    const parsedType = this._getType(input);
    if (parsedType === z.ZodParsedType.undefined) {
      return { status: 'valid', value: undefined };
    }
    return this._def.innerType._parse(input);
  }
  unwrap(): T {
    return this._def.innerType;
  }
  static create<T_1 extends z.ZodTypeAny>(type: T_1, params?: RawCreateParams) {
    return new ZodColumnOptional({
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

export class StringCol extends ZodColumn<ZodType<string, z.ZodStringDef>> {
  sqlType: ColumnDataType = 'text';
  _parse(input: z.ParseInput): z.ParseReturnType<any> {
    const t = this;
    return z.ZodString.prototype._parse.call(this, input);
  }
  static create(params?: RawCreateParams) {
    return new StringCol({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodString,
      ...processCreateParams(params)
    } as z.ZodStringDef);
  }
}

class IntCol extends ZodColumn<ZodType<number, z.ZodNumberDef>> {
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
    return new IntCol({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodNumber,
      ...processCreateParams(params)
    } as z.ZodNumberDef);
  }
}

class NumericCol extends ZodColumn {
  sqlType: ColumnDataType = 'numeric';
  _parse(input: z.ParseInput): z.ParseReturnType<any> {
    // custom parse
    if (!Number.isFinite(input.data))
      return {
        status: 'dirty',
        value: input.data
      };
    // then use zods parser
    return z.ZodNumber.prototype._parse.call(this, input);
  }

  static create(params?: RawCreateParams) {
    return new IntCol({
      checks: [],
      typeName: z.ZodFirstPartyTypeKind.ZodNumber,
      ...processCreateParams(params)
    } as z.ZodNumberDef);
  }
}

export namespace Columns {
  export const text = StringCol.create;
  export const int = IntCol.create;
  //   const s = z.string();
  //   const p1 = addProp(s, '_colData', {});
  //   const p2 = addProp(p1, 'primaryKey', (() => this).bind(p1));
  // };
}

// type ZodCol<T extends z.ZodType> = T & AddedColData;

// type ZodColOpti<T extends z.ZodType> = z.ZodOptional<T> & AddedColData;
// // type ZodColFunc<T extends z.ZodType> = (colDef?: ColArgs) => ZodCol<T>;
// // type ZodColFuncOpti<T extends z.ZodType> = (colDef?: ColArgs) => ZodColOpti<T>;

// // const _genColFn =
// //   <T extends z.ZodType>(zf: T, sqlType: ColumnDataType): ZodColFunc<T> =>
// //   (colDef: ColArgs = {}) =>
// //     addProp(
// //       !colDef.required ? z.optional(zf) : zf,
// //       'colDef' as keyof AddedColData,
// //       { ...colDef, dataType: sqlType } as ColumnDef
// //     );

// const _genColFn =
//   <T extends z.ZodType>(zf: T, sqlType: ColumnDataType) =>
//   (colDef: ColArgs) => {
//     if (tg(zf, colDef)) {
//       return addProp(
//         zf,
//         'colDef' as keyof AddedColData,
//         { ...colDef, dataType: sqlType } as AddedColData['colDef']
//       );
//     }
//     if (colDef['required'] === true || colDef['primaryKey'] === true) {
//       return addProp(
//         zf,
//         'colDef' as keyof AddedColData,
//         { ...colDef, dataType: sqlType } as AddedColData['colDef']
//       );
//     }
//     return addProp(
//       z.optional(zf),
//       'colDef' as keyof AddedColData,
//       { ...colDef, dataType: sqlType } as AddedColData['colDef']
//     );
//   };

// export namespace Columns {
//   export const text = (col: ColArgs) => {
//     if (col.required)
//       return addProp(
//         z.string(),
//         'colDef' as keyof AddedColData,
//         { ...col, dataType: 'text' } as AddedColData['colDef']
//       );
//     return addProp(
//       z.string().optional(),
//       'colDef' as keyof AddedColData,
//       { ...col, dataType: 'text' } as AddedColData['colDef']
//     );
//   };

//   export const int = _genColFn(
//     z.number().refine(v => Number.isInteger(v)),
//     'integer'
//   );

//   export const binary = _genColFn(
//     z.custom<Buffer>(data => {
//       if (!(data instanceof Buffer)) {
//         return false;
//       }
//       return true;
//     }),
//     'binary'
//   );
// }
