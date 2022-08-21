import { ColumnDataType, sql } from 'kysely';
import { z } from 'zod';
import { addProp } from '../util';

export interface ColumnDef {
  name: string;
  dataType: ColumnDataType;
  required?: boolean;
  primaryKey?: boolean;
  // default calculated server-side: pass in raw sql value with the template literal
  defaultVal?: ReturnType<typeof sql>;
}

type ColArgs = Omit<ColumnDef, 'name' | 'dataType'>;
type AddedColData = { colDef: Omit<ColumnDef, 'name'> };

type ZodCol<T extends z.ZodType> = T & AddedColData;

type ZodColOpti<T extends z.ZodType> = z.ZodOptional<T> & AddedColData;
// type ZodColFunc<T extends z.ZodType> = (colDef?: ColArgs) => ZodCol<T>;
// type ZodColFuncOpti<T extends z.ZodType> = (colDef?: ColArgs) => ZodColOpti<T>;

// const _genColFn =
//   <T extends z.ZodType>(zf: T, sqlType: ColumnDataType): ZodColFunc<T> =>
//   (colDef: ColArgs = {}) =>
//     addProp(
//       !colDef.required ? z.optional(zf) : zf,
//       'colDef' as keyof AddedColData,
//       { ...colDef, dataType: sqlType } as ColumnDef
//     );

const _genColFn =
  <T extends z.ZodType>(zf: T, sqlType: ColumnDataType) =>
  (colDef: ColArgs) => {
    if (tg(zf, colDef)) {
      return addProp(
        zf,
        'colDef' as keyof AddedColData,
        { ...colDef, dataType: sqlType } as AddedColData['colDef']
      );
    }
    if (colDef['required'] === true || colDef['primaryKey'] === true) {
      return addProp(
        zf,
        'colDef' as keyof AddedColData,
        { ...colDef, dataType: sqlType } as AddedColData['colDef']
      );
    }
    return addProp(
      z.optional(zf),
      'colDef' as keyof AddedColData,
      { ...colDef, dataType: sqlType } as AddedColData['colDef']
    );
  };

export namespace Columns {
  export const text = (col: ColArgs) => {
    if (col.required)
      return addProp(
        z.string(),
        'colDef' as keyof AddedColData,
        { ...col, dataType: 'text' } as AddedColData['colDef']
      );
    return addProp(
      z.string().optional(),
      'colDef' as keyof AddedColData,
      { ...col, dataType: 'text' } as AddedColData['colDef']
    );
  };

  export const int = _genColFn(
    z.number().refine(v => Number.isInteger(v)),
    'integer'
  );

  export const binary = _genColFn(
    z.custom<Buffer>(data => {
      if (!(data instanceof Buffer)) {
        return false;
      }
      return true;
    }),
    'binary'
  );
}
