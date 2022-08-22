import assert from 'assert';
import {z} from 'zod';
import {Columns} from '../src/column';
import {InferSchema, ZsqlSchema} from '../src/schema';
import {table} from '../src/table';
import {expectType} from '../src/util';
import {getTestDb} from './helpers';

const shape = {
  t1: {
    a: Columns.text(),
    b: Columns.int().optional()
  },
  t2: {
    c: Columns.int().primaryKey()
  }
};
describe('schema', () => {
  [
    {
      create: () => ZsqlSchema.getSchemaDefFn(getTestDb())(shape),
      casename: 'schema.getSchemaDefFn'
    },
    {
      create: () => ZsqlSchema.create(shape, {db: getTestDb()}),
      casename: 'schema.create'
    }
  ].forEach(c => {
    describe(`schema created thru ${c.casename}`, () => {
      const schema = c.create();
      it('schema typings', () => {
        type T = InferSchema<typeof schema>;
        expectType<T['t1']['a']>('asdf');
        expectType<T['t1']['b']>(undefined);
        expectType<T['t2']['c']>(1);
      });

      it('getDb', async () => {
        const db = schema.getDb();

        const rows = await db
          .selectFrom('t1')
          .select(['a', 't1.a'])
          .executeTakeFirst();
      });
    });
  });
});
