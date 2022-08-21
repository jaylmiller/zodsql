import { z } from 'zod';
import { Columns } from '../src/ddl/column';
type TypesAreEqual2<T, U> = [T] extends [U]
  ? [U] extends [T]
    ? any
    : never
  : never;
describe('ddl', () => {
  describe('column', () => {
    it('typings work', () => {
      // this test should only compile when the typings are correct
      const o = z.object({
        scol: Columns.text({}),
        bincol: Columns.binary({})
      });

      type T = z.infer<typeof o>;
      // cant assign stuff to never
      let a: TypesAreEqual2<T['bincol'], Buffer | undefined> = 1;
      let b: TypesAreEqual2<T['scol'], string | undefined> = 1;
      const o2 = z.object({
        scol: Columns.text({ required: true }),
        bincol: Columns.binary({ primaryKey: true })
      });
      type T2 = z.infer<typeof o2>;
      let c: TypesAreEqual2<T2['scol'], string> = 1;
    });
  });
});
