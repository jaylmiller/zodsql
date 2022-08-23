import assert from 'assert';
import {z} from 'zod';
import {Columns} from '../src/column';
import {InferSchema, ZsqlSchema} from '../src/schema';
import {createTableFn, table} from '../src/table';
import {expectType} from '../src/util';
import {getTestDb} from './helpers';

const shape = {
  a: Columns.text().primaryKey(),
  b: Columns.int(),
  c: Columns.binary().optional()
};
describe('table', () => {
  [
    {
      getTable: () => table('testy', shape).bindDb(getTestDb()),
      casename: 'table.create'
    },
    {
      getTable: () => createTableFn(getTestDb())('testy', shape),
      casename: 'table.getNewTableFn'
    }
  ].forEach(t => {
    describe(`return value of: ${t.casename}`, () => {
      const testTable = t.getTable();
      it('parses', () => {
        testTable.parse({
          a: 'asdf',
          b: 1
        });
        const bad = testTable.safeParse({
          a: 'asdf',
          b: 'a'
        });
        assert.ok(!bad.success);
      });
      afterEach(() => {
        testTable.getDb().destroy();
        testTable.bindDb(getTestDb());
      });
      it('typing works', () => {
        type T = z.infer<typeof testTable>;
        expectType<T['a']>('');
        expectType<T['b']>(1);
        expectType<T['c']>(Buffer.alloc(1));
        expectType<T['c']>(undefined);
      });

      it('sqlData', () => {
        const sqlData = testTable._getSqlData();
        assert.deepEqual(
          sqlData.cols.map(c => c.name),
          ['a', 'b', 'c']
        );
        assert(sqlData.cols[0].primaryKey);
        assert.equal(sqlData.cols[0].dataType, 'text');
        assert(!sqlData.cols[2].required);
        const tschema = table('test', {
          a: Columns.text().primaryKey(),
          b: Columns.int(),
          c: Columns.binary().optional()
        });
      });

      it('ddl', async () => {
        const ddl = testTable.ddl();
        await ddl.execute();
        const db = testTable.getDb();
        const tables = await db.introspection.getTables();
        assert.equal(tables.length, 1);
        assert.equal(tables[0].name, 'testy');
        // kysely returns columns in alphabetical order for some reason
        const pkCol = tables[0].columns.find(c => c.name === 'a');
        assert.equal(pkCol?.dataType.toLowerCase(), 'text');
      });

      it('insert one', async () => {
        await testTable.ddl().execute();
        const stmt = testTable.insert({a: 'testy', b: 1, c: Buffer.alloc(1)});
        const res = await stmt.execute();
        return;
      });

      it('insert many', async () => {
        await testTable.ddl().execute();
        const stmt = testTable.insert([
          {a: 'testy', b: 1, c: Buffer.alloc(1)},
          {a: 'asdf', b: 3},
          {a: 'asdfasdf', b: 4}
        ]);
        const res = await stmt.execute();
        const rows = await testTable
          .getDb()
          .selectFrom(testTable.name)
          .selectAll()
          .execute();
        assert.equal(rows.length, 3);
        return;
      });
    });
  });
});
