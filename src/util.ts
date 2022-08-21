import assert from 'assert';

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
