import assert from 'assert';
import { z } from 'zod';
/**
 * Get key value pairs of an object, like python's dict.items
 * @param obj
 * @returns array of [k,v] tuples
 */
export const objItems = <T extends object>(
  obj: T
): Array<[keyof T, T[keyof T]]> =>
  Object.getOwnPropertyNames(obj).map(k => {
    keyIsProp(k, obj);
    return [k, obj[k]];
  });

/**
 * Adds a new property to the base object and returns it.
 * @param base
 * @param propertyName
 * @param propertyVal
 * @returns
 */
export const addProps = <T extends object, K extends string, V>(
  base: T,
  propertyNames: K[],
  propertyVals: V[]
): T & { [p in K]: V } => {
  assert(propertyNames.length === propertyVals.length);
  const sourceObj = propertyNames.reduce(
    (acc, cur, idx) => ({
      ...acc,
      [cur]: propertyVals[idx]
    }),
    {} as { [p in K]: V }
  );
  return Object.assign(base, sourceObj);
};

export const addProp = <T extends object, K extends string, V>(
  base: T,
  propertyName: K,
  propertyVal: V
): T & { [p in K]: V } =>
  Object.assign<T, { [p in K]: V }>(base, {
    [propertyName]: propertyVal
  } as { [p in K]: V });

export function keyIsProp<O extends object>(
  key: string | number | symbol,
  obj: O
): asserts key is keyof O {
  assert(Object.keys(obj).includes(key as any));
}

/**
 * CODE BELOW IS COPIED DIRECTLY FROM ZOD SOURCE.
 * (i.e. they were not exported by the package but we need them)
 */

// copied from zod source code since it wasnt exported anywhere
export function processCreateParams(params: any) {
  if (!params) return {};
  const { errorMap, invalid_type_error, required_error, description } = params;
  if (errorMap && (invalid_type_error || required_error)) {
    throw new Error(
      `Can't use "invalid" or "required" in conjunction with custom error map.`
    );
  }
  if (errorMap) return { errorMap: errorMap, description };
  const customMap = (iss: any, ctx: any) => {
    if (iss.code !== 'invalid_type') return { message: ctx.defaultError };
    if (typeof ctx.data === 'undefined') {
      return {
        message:
          required_error !== null && required_error !== void 0
            ? required_error
            : ctx.defaultError
      };
    }
    return {
      message:
        invalid_type_error !== null && invalid_type_error !== void 0
          ? invalid_type_error
          : ctx.defaultError
    };
  };
  return { errorMap: customMap, description };
}

/**
 *
 */
export const INVALID = Object.freeze({
  status: 'aborted'
});

/**
 * helper code copied from zod source code /src/helpers/parseUtil.ts
 * (not exported in package)
 */
export function addIssueToContext(
  ctx: z.ParseContext,
  issueData: z.IssueData
): void {
  const issue = z.makeIssue({
    issueData: issueData,
    data: ctx.data,
    path: ctx.path,
    errorMaps: [
      ctx.common.contextualErrorMap, // contextual error map is first priority
      ctx.schemaErrorMap, // then schema-bound map if available
      z.getErrorMap(), // then global override map
      z.defaultErrorMap // then global default map
    ].filter(x => !!x) as z.ZodErrorMap[]
  });
  ctx.common.issues.push(issue);
}
