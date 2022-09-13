import assert from 'assert';
import {expectType, objFilter} from '../src/util';

it('objFilter', () => {
  const obj = {
    a: 1,
    b: 2,
    c: 3
  };
  const fn = (n: number) => n === 1;
  const newobj = objFilter(obj, fn);
  assert.equal(Object.keys(newobj).length, 1);
  expectType<typeof newobj['c']>(1);
});
