import {Kysely, SqliteDialect} from 'kysely';
import Database from 'better-sqlite3';

export const getTestDb = <T = any>() =>
  new Kysely<T>({
    dialect: new SqliteDialect({
      database: new Database(':memory:')
    })
  });
