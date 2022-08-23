import assert from 'assert';
import {z} from 'zod';
import {columns} from '../src/column';
import {InferSchema, ZsqlSchema, _InferSchemaRaw} from '../src/schema';
import {table} from '../src/table';
import {expectType} from '../src/util';
import {getTestDb} from './helpers';

const shape = {
  t1: {
    a: columns.text(),
    b: columns.int().optional()
  },
  t2: {
    c: columns.int().primaryKey()
  }
};
describe('schema', () => {
  [
    {
      create: () => ZsqlSchema.getNewSchemaFn(getTestDb())(shape),
      casename: 'schema.getSchemaDefFn'
    },
    {
      create: () => ZsqlSchema.create(shape, {db: getTestDb()}),
      casename: 'schema.create'
    }
  ].forEach(c => {
    describe(`schema created thru ${c.casename}`, () => {
      const schema = c.create();
      afterEach(() => {
        schema.getFullDbApi().destroy();
        schema.bindDb(getTestDb());
      });
      it('schema typings', () => {
        type T = _InferSchemaRaw<typeof schema['tables']>;
        expectType<T['t1']['a']>('asdf');
        expectType<T['t1']['b']>(undefined);
        expectType<T['t2']['c']>(1);
      });

      it('ddl', async () => {
        const db = schema.getFullDbApi();
        for (let ddl of schema.ddl()) {
          await ddl.execute();
        }
        for (let ddl of schema.ddl()) {
          await ddl.execute();
        }
      });

      it('select typings', async () => {
        const db = schema.getDb();
        for (let ddl of schema.ddl()) {
          await ddl.execute();
        }
        await schema.tables.t1.insert([{a: 'a'}, {a: 'b', b: 12}]).execute();
        const rows = await db.selectFrom('t1').select(['a', 't1.a']).execute();
        assert.ok(rows.map(r => r.a).includes('b'));
        type RowT = typeof rows[0];
        expectType<RowT['a']>('');
        expectType<RowT extends {'t1.a': string; a: string} ? false : true>(
          true
        );
      });
    });
  });
});
