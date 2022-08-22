import assert from 'assert';

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
): T & {[p in K]: V} => {
  assert(propertyNames.length === propertyVals.length);
  const sourceObj = propertyNames.reduce(
    (acc, cur, idx) => ({
      ...acc,
      [cur]: propertyVals[idx]
    }),
    {} as {[p in K]: V}
  );
  return Object.assign(base, sourceObj);
};

export const addProp = <T extends object, K extends string, V>(
  base: T,
  propertyName: K,
  propertyVal: V
): T & {[p in K]: V} =>
  Object.assign<T, {[p in K]: V}>(base, {
    [propertyName]: propertyVal
  } as {[p in K]: V});
