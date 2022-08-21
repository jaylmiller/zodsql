import assert from 'assert';
import { z } from 'zod';
import { Columns, StringCol, ZodColumnOptional } from '../src/ddl/column';
type TypesAreEqual2<T, U> = [T] extends [U]
  ? [U] extends [T]
    ? any
    : never
  : never;
describe('ddl', () => {
  describe('column', () => {
    it('primitive cols parses', () => {
      const i = Columns.int();
      const s = Columns.text();
      const parsed = s.primaryKey().parse('asdf');
      assert.equal(parsed, 'asdf');
      const parsedint = i.optional().parse(1);
      assert.equal(parsedint, 1);
      assert.throws(() => i.parse(0.1), z.ZodError);
    });

    it('optional gets methods', () => {
      const i = Columns.int();
      const uu = i.optional().unique();
      assert(uu instanceof ZodColumnOptional);
      // assert.equal(parsedint, 1);
    });

    it('works with object', () => {
      const o = z.object({
        s: Columns.text(),
        i: Columns.int()
      });

      type T = z.infer<typeof o>;
      const a = o.parse({ s: 'asdf', i: 1 });

      console.log(a);
    });
  });
});
