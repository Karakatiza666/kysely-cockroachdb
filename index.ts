import { Kysely, DialectAdapter, PostgresDialect, MigrationLockOptions, KyselyPlugin } from "kysely"

class CockroachAdapter implements DialectAdapter {
   #schemaPlugin!: KyselyPlugin

   get supportsTransactionalDdl() {
      return true;
   }

   get supportsReturning() {
      return true;
   }

   // async acquireMigrationLock(db: Kysely<any>, options: MigrationLockOptions) {
   //    // https://github.com/kysely-org/kysely/blob/master/src/migration/migrator.ts
   //    await db.selectFrom(options.lockTable).selectAll().forUpdate().execute()
   // }

   // Arrow function to capture `this` of Migrator
   acquireMigrationLock = async (db: Kysely<any>, options: MigrationLockOptions) => {
      // https://github.com/kysely-org/kysely/blob/master/src/migration/migrator.ts
      // await db.withPlugin(this.#schemaPlugin).selectFrom(options.lockTable).selectAll().forUpdate().execute()
      await db.selectFrom(options.lockTable).selectAll().forUpdate().execute()
   }

   async releaseMigrationLock() {
      // Nothing to do here. The "for update" lock is automatically released at the
      // end of the transaction and since `supportsTransactionalDdl` true, we know
      // the `db` instance passed to acquireMigrationLock is actually a transaction.
   }
}

export class CockroachDialect extends PostgresDialect {
   createAdapter() {
      return new CockroachAdapter()
   }
}