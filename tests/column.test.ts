import assert from 'assert';
import {z, ZodEffects} from 'zod';
import {columns, ZsqlColumnOptional, ZsqlColumn} from '../src/column';
import {ZsqlTable} from '../src/table';
type TypesAreEqual2<T, U> = [T] extends [U]
  ? [U] extends [T]
    ? any
    : never
  : never;
const ser = (o: any) => JSON.parse(JSON.stringify(o));
const colTests = [
  {
    c: columns.text,
    good: 'asdf',
    bad: [1, true, 0.1]
  },
  {
    c: columns.int,
    good: 1,
    bad: ['a', true, 0.5]
  },
  {
    c: columns.numeric,
    good: 0.1,
    bad: ['asdf', true]
  },
  {
    c: columns.timestamp,
    good: new Date('1970-01-01'),
    bad: ['asdfasdf', 1, false]
  },
  {
    c: columns.date,
    good: new Date('1970-01-01'),
    bad: ['asdfasdf', 1, false]
  },
  {
    c: columns.binary,
    good: Buffer.alloc(1),
    bad: ['asdfasdf', 1]
  },
  {
    c: columns.bigint,
    good: BigInt('123485812351231235'),
    bad: ['a', 0, false]
  },
  {
    c: columns.jsonb,
    good: JSON.stringify({asdf: 'hi'}),
    bad: ['{"']
  },
  {
    c: columns.jsonb,
    good: {asdf: 'hi'},
    bad: ['{"']
  }
  // {
  //   c: columns.jsonb,
  //   good: {hi: 'hi'},
  //   bad: ['{"']
  // }
];
for (let d of colTests) {
  const {c, good, bad} = d;
  describe(`column: ${c().sqlType} (wraps: ${c().zodType.name})`, () => {
    it(`stores column data`, () => {
      const t = c().sqlType;
      assert.deepEqual(c()._columnData(), {
        required: true,
        dataType: t
      });
      // primary key should set true
      assert.deepEqual(c().primaryKey()._columnData(), {
        required: true,
        primaryKey: true,
        dataType: t
      });
      // make sure it returns new objects instead of mutating in place
      const s = c();
      assert.deepEqual(c().unique()._columnData(), {
        required: true,
        unique: true,
        dataType: t
      });
      assert.deepEqual(s._columnData(), {
        required: true,
        dataType: t
      });
      assert.deepEqual(c().optional()._columnData(), {
        required: false,
        dataType: t
      });

      // should not be able to make primary keys optional
      assert.throws(() => c().primaryKey().optional());
    });
    it('copySelf', () => {
      const i = c().primaryKey();
      const copy = i.copy();
      assert(copy instanceof Object.getPrototypeOf(i).constructor);
      assert.equal(copy._columnData().primaryKey, true);
    });
    it(`optional works and gets methods`, () => {
      const i = c();
      const uu = i.optional().unique();
      assert(uu instanceof ZsqlColumnOptional);
      assert(uu.isOptional());
      const parsed = uu.parse(undefined);
      assert.ok(typeof parsed === 'undefined');
      assert.ok(!uu._columnData().required);
    });
    it(`parsing `, () => {
      assert.deepEqual(c().parse(good), good);
      assert.deepEqual(c().primaryKey().parse(good), good);
      for (let v of bad) {
        const sp = c().safeParse(v);
        assert(!sp.success);
        assert.throws(() => c().parse(v), z.ZodError);
      }
    });
    it(`within object`, () => {
      const keys = ['k1', 'k2', 'k3'];
      const o = z.object(keys.reduce((acc, cur) => ({...acc, [cur]: c()}), {}));
      const parsed = o.parse(
        keys.reduce((acc, cur) => ({...acc, [cur]: good}), {})
      ) as any;
      keys.forEach(k => {
        assert.deepEqual(parsed[k], good);
      });

      assert.throws(
        () => o.parse(keys.reduce((acc, cur) => ({...acc, [cur]: bad[0]}), {})),
        z.ZodError
      );
    });
    it('toZodType', () => {
      const col = c();
      const zreg = col.toZodType();
      assert(zreg instanceof col.zodType);
      const zopt = col.optional().toZodType();
      assert(zopt._def.innerType instanceof col.zodType);
      return;
    });
  });
}
it('customcol', () => {
  const col = columns.custom<string>({sqlType: 'serial'});
  type cc = z.infer<typeof col>;
  const parsed = col.parse('asdf');
});
it('unsupported methods escape to original zod class', () => {
  // when a method that is not supported is invoked, make sure it returns the correct
  // underlying type
  const s = columns.text();
  const refs = s.refine(s => s === 'asdf');
  const sp = refs.safeParse('a');
  assert(!sp.success);
  // chain refines
  const ss = s.refine(a => a.length >= 4).refine(a => 'asdf');
  ss.parse('asdf');

  // try with custom binary type
  const bin = columns.binary().primaryKey();
  // bin.parse(Buffer.alloc(4));
  const refd = bin.refine(b => b.length >= 4);
  const r = refd.parse(Buffer.alloc(4));
});
it('typings work with object', () => {
  // if typings dont work this test block wont compile
  const o = z.object({
    s: columns.text(),
    i: columns.int(),
    so: columns.text().optional()
  });
  type T = z.infer<typeof o>;
  // let a1: T['s'] extends string ? true : false = true;
  // let a2: T['so'] extends string ? true : false = false;
  // let a3: T['i'] extends number ? true : false = true;
  let stest: TypesAreEqual2<T['s'], string> = '1';
  let itest: TypesAreEqual2<T['i'], number> = '1';
  let sotest: TypesAreEqual2<T['so'], string | undefined> = '1';
  const a = o.parse({s: 'asdf', i: 1});
  assert.deepEqual(a, {s: 'asdf', i: 1});
});
