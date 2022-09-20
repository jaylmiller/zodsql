import assert from 'assert';
import {z} from 'zod';
import {columns, ZsqlColumn} from '../src/column';
import {InferSchema, ZsqlSchema} from '../src/schema';
import {createTableFn, PrimaryKey, table} from '../src/table';
import {expectType} from '../src/util';
import {getTestDb} from './helpers';

const shape = {
  a: columns.text().primaryKey(),
  b: columns.int(),
  c: columns.binary().optional()
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
          a: columns.text().primaryKey(),
          b: columns.int(),
          c: columns.binary().optional()
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

      it('from pk', async () => {
        await testTable.ddl().execute();
        const stmt = testTable.insert({a: 'testy', b: 1, c: Buffer.alloc(1)});
        const res = await stmt.execute();
        const obj = await testTable.fromPk({a: 'testy'});
        assert.equal(obj?.b, 1);
        const nobj = await testTable.fromPk({a: 't'});
        assert.equal(typeof nobj, 'undefined');
      });

      it('selectWithValues', async () => {
        await testTable.ddl().execute();
        await testTable
          .insert({a: 'testy', b: 1, c: Buffer.alloc(1)})
          .execute();
        await testTable
          .insert({a: 'testy2', b: 2, c: Buffer.alloc(1)})
          .execute();
        await testTable
          .insert({a: 'testy3', b: 1, c: Buffer.alloc(1)})
          .execute();
        const res = await testTable.selectWithValues({a: 'testy', b: 1});
        assert.equal(res.length, 1);
      });

      it('to zod', async () => {
        const zshape = testTable.toZodShape();
        assert(zshape.a instanceof z.ZodString);
        assert(zshape.b instanceof z.ZodNumber);
        assert(zshape.c instanceof z.ZodOptional);
        assert(zshape.c._def.innerType instanceof z.ZodAny);
        const zt = testTable.toZodType();
        assert(zt instanceof z.ZodObject);
        type ZT = z.infer<typeof zt>;
        expectType<ZT['a']>('');
      });
      it('cust cols', () => {
        const tab = table('testy', {
          cust: columns.custom<string>({sqlType: 'bigint'})
        }).bindDb(getTestDb());
        const compiled = tab.ddl().compile();
        assert.ok(compiled.sql.includes('bigint'));
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
