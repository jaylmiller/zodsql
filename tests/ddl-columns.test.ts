import assert from 'assert';
import { z, ZodEffects } from 'zod';
import { Columns, ZsqlColumnOptional, ZsqlColumn } from '../src/ddl/column';
type TypesAreEqual2<T, U> = [T] extends [U]
  ? [U] extends [T]
    ? any
    : never
  : never;
const colTests = [
  {
    c: Columns.text,
    good: 'asdf',
    bad: 1
  },
  {
    c: Columns.int,
    good: 1,
    bad: 0.5
  },
  {
    c: Columns.numeric,
    good: 0.1,
    bad: 'asdf'
  },
  {
    c: Columns.timestamp,
    good: new Date('1970-01-01'),
    bad: 'asdfasdf'
  },
  {
    c: Columns.date,
    good: new Date('1970-01-01'),
    bad: 'asdfasdf'
  },
  {
    c: Columns.binary,
    good: Buffer.alloc(1),
    bad: 'asdfasdf'
  }
];
describe('column', () => {
  for (let d of colTests) {
    const { c, good, bad } = d;
    describe(`sqlType: ${c().sqlType}`, () => {
      it(`stores column data`, () => {
        const t = c().sqlType;
        assert.deepEqual(c()._getColData(), {
          required: true,
          dataType: t
        });
        // primary key should set true
        assert.deepEqual(c().primaryKey()._getColData(), {
          required: true,
          primaryKey: true,
          dataType: t
        });
        // make sure it returns new objects instead of mutating in place
        const s = c();
        assert.deepEqual(c().unique()._getColData(), {
          required: true,
          unique: true,
          dataType: t
        });
        assert.deepEqual(s._getColData(), {
          required: true,
          dataType: t
        });
        assert.deepEqual(c().optional()._getColData(), {
          required: false,
          dataType: t
        });

        // should not be able to make primary keys optional
        assert.throws(() => c().primaryKey().optional());
      });
      it(`optional works and gets methods`, () => {
        const i = c();
        const uu = i.optional().unique();
        assert(uu instanceof ZsqlColumnOptional);
        assert(uu.isOptional());
        const parsed = uu.parse(undefined);
        assert.ok(typeof parsed === 'undefined');
        assert.ok(!uu._getColData().required);
      });
      it(`parsing `, () => {
        assert.deepEqual(c().parse(good), good);
        assert.deepEqual(c().primaryKey().parse(good), good);
        const sp = c().safeParse(bad);
        assert(!sp.success);
        assert.throws(() => c().parse(bad), z.ZodError);
      });
      it(`within object`, () => {
        const keys = ['k1', 'k2', 'k3'];
        const o = z.object(
          keys.reduce((acc, cur) => ({ ...acc, [cur]: c() }), {})
        );
        const parsed = o.parse(
          keys.reduce((acc, cur) => ({ ...acc, [cur]: good }), {})
        ) as any;
        keys.forEach(k => {
          assert.deepEqual(parsed[k], good);
        });
        assert.throws(
          () =>
            o.parse(keys.reduce((acc, cur) => ({ ...acc, [cur]: bad }), {})),
          z.ZodError
        );
      });
    });
  }

  it('typings work with object', () => {
    // if typings dont work this next line wont compile
    const o = z.object({
      s: Columns.text(),
      i: Columns.int(),
      so: Columns.text().optional()
    });
    type T = z.infer<typeof o>;
    let stest: TypesAreEqual2<T['s'], string> = '1';
    let itest: TypesAreEqual2<T['i'], number> = '1';
    let sotest: TypesAreEqual2<T['so'], string | undefined> = '1';
    const a = o.parse({ s: 'asdf', i: 1 });
    assert.deepEqual(a, { s: 'asdf', i: 1 });
  });
});
