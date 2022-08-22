import assert from 'assert';
import { z } from 'zod';
import { Columns } from '../src/ddl/column';
import { InferSchema, ZsqlSchema } from '../src/ddl/schema';
import { table } from '../src/ddl/table';
import { expectType } from '../src/util';

describe.only('table', () => {
  const t = table('testy2', {
    a: Columns.text().primaryKey(),
    b: Columns.int(),
    c: Columns.binary().optional()
  });
  it('parses', () => {
    const parsed = t.parse({
      a: 'asdf',
      b: 1
    });
  });
  it('typing works', () => {
    type T = z.infer<typeof t>;
    expectType<T['a']>('');
    expectType<T['b']>(1);
    expectType<T['c']>(Buffer.alloc(1));
    expectType<T['c']>(undefined);
  });

  it('sqlData', () => {
    const sqlData = t._getSqlData();
    assert.deepEqual(
      sqlData.cols.map(c => c.name),
      ['a', 'b', 'c']
    );
    assert(sqlData.cols[0].primaryKey);
    assert.equal(sqlData.cols[0].dataType, 'text');
    assert(!sqlData.cols[2].required);
    const tschema = table(
      { name: 'test', schema: 'testschema' },
      {
        a: Columns.text().primaryKey(),
        b: Columns.int(),
        c: Columns.binary().optional()
      }
    );
    assert.equal(tschema._getSqlData().schema, 'testschema');
  });
});
describe('schema', () => {
  it('schema typings', () => {
    const a = ZsqlSchema.create({
      t1: {
        a: Columns.text(),
        b: Columns.int().optional()
      },
      t2: {
        c: Columns.int().primaryKey()
      }
    });
    type T = InferSchema<typeof a>;
    expectType<T['t1']['a']>('asdf');
    expectType<T['t1']['b']>(undefined);
    expectType<T['t2']['c']>(1);
  });
});
