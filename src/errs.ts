export class NoDatabaseFound extends Error {}

export class TableNotFound extends Error {
  constructor(name: string) {
    super(`Table "${name}" not found!`);
  }
}
