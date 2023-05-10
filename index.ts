import { Kysely, DialectAdapter, PostgresDialect, MigrationLockOptions, KyselyPlugin, QueryCompiler, PostgresQueryCompiler, SchemaModule, CreateTableBuilder, QueryExecutor, ColumnBuilderCallback, ColumnDefinitionBuilder, ColumnDefinitionNode, GeneratedNode, OperationNode, ColumnNode, CreateTableBuilderProps, Expression, OnModifyForeignAction, ColumnDataType, Transaction, TransactionBuilder } from "kysely"

// import { createQueryId } from 'kysely/dist/cjs/util/query-id'
// import { CreateTableNode } from 'kysely/dist/cjs/operation-node/create-table-node'
// import { parseTable } from 'kysely/dist/cjs/parser/table-parser'
// import { DataTypeExpression, parseDataTypeExpression } from "kysely/dist/cjs/parser/data-type-parser"
// import { noop, freeze } from 'kysely/dist/cjs/util/object-utils'
// import { DefaultValueExpression } from "kysely/dist/cjs/parser/default-value-parser"

type Arg1<T> = T extends (arg1: infer U, ...args: any[]) => any ? U : never;
type Arg2<T> = T extends (arg1: any, arg2: infer U, ...args: any[]) => any ? U : never;

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
   createQueryCompiler() {
      return new CockroachQueryCompiler()
   }
}

export class CockroachQueryCompiler extends PostgresQueryCompiler {
   visitGenerated(node: CockroachGeneratedNode) {
      this.append('generated ');
      if (node.always) {
         this.append('always ');
      }
      if (node.byDefault) {
         this.append('by default ');
      }
      this.append('as ');
      if (node.identity) {
         this.append('identity');
      }
      if (node.expression) {
         this.append('(');
         this.visitNode(node.expression);
         this.append(')');
      }
      if (node.stored) {
         this.append(' stored');
      }
      if (node.virtual) {
         this.append(' virtual');
      }
   }
}

function getCallerName() {
   // Get stack array
   // const orig = Error.prepareStackTrace;
   // Error.prepareStackTrace = (error, stack) => stack;
   // const { stack } = new Error();
   // Error.prepareStackTrace = orig;
   
   // const caller = stack[2];
   // return caller ? caller.getFunctionName() : undefined;
   return '><'
}

export class KyselyCockroach<DB> extends Kysely<DB> {
   get schema(): CockroachSchemaModule {
      console.log('get schema')
      return Object.setPrototypeOf(super.schema, CockroachSchemaModule.prototype)
   }
   withPlugin(x: KyselyPlugin): KyselyCockroach<DB> {
      console.log('withPlugin', getCallerName())
      return Object.setPrototypeOf(super.withPlugin(x), KyselyCockroach.prototype)
   }
   withoutPlugins(): KyselyCockroach<DB> {
      console.log('withoutPlugins', getCallerName())
      return Object.setPrototypeOf(super.withoutPlugins(), KyselyCockroach.prototype)
   }
   withTables<T extends Record<string, Record<string, any>>>(): KyselyCockroach<DB & T> {
      console.log('withTables')
      return Object.setPrototypeOf(super.withTables(), KyselyCockroach.prototype);
   }
   transaction(): CockroachTransactionBuilder<DB> {
      return Object.setPrototypeOf(super.transaction(), CockroachTransactionBuilder.prototype) 
      // const txBuilder = super.transaction()
      // return {
      //    setIsolationLevel: txBuilder.setIsolationLevel.bind(txBuilder),
      //    execute(cb) { 
      //       return txBuilder.execute(tx => {
      //          console.log('in tx execute', tx)
      //          Object.setPrototypeOf(tx, CockrachTransaction.prototype)
      //          console.log('callback tx execute', tx)
      //          return cb(tx)
      //       })
      //    }
      // }
   }
}

// ====

class CockroachTransactionBuilder<DB> extends TransactionBuilder<DB> {
   execute<T>(callback: (trx: Transaction<DB>) => Promise<T>): Promise<T> {
      return super.execute(tx => {
         console.log('in tx execute', tx)
         return callback(Object.setPrototypeOf(tx, CockrachTransaction.prototype))
      })
   }
}

// ====

class CockrachTransaction<DB> extends Transaction<DB> {
   get schema(): CockroachSchemaModule {
      console.log('tx get schema')
      // return new CockroachSchemaModule(this.getExecutor());
      return Object.setPrototypeOf(super.schema, CockroachSchemaModule.prototype)
   }

   withPlugin(plugin: KyselyPlugin): CockrachTransaction<DB> {
      console.log('tx withPlugin')
      return Object.setPrototypeOf(super.withPlugin(plugin), CockrachTransaction.prototype)
   }
   withoutPlugins(): CockrachTransaction<DB> {
      console.log('tx withoutPlugins')
      return Object.setPrototypeOf(super.withoutPlugins(), CockrachTransaction.prototype)
   }
   withTables<T extends Record<string, Record<string, any>>>(): CockrachTransaction<DB & T> {
      console.log('tx withTables')
      return Object.setPrototypeOf(super.withTables(), CockrachTransaction.prototype)
   }
}

// ====

class CockroachSchemaModule extends SchemaModule {
   // #executor!: QueryExecutor
   // constructor(executor: QueryExecutor) {
   //    super(executor)
   //    this.#executor = executor
   // }

   createTable<TB extends string>(table: TB): CockroachCreateTableBuilder<TB, never> {
      // return new CockroachCreateTableBuilder({
      //    queryId: createQueryId(),
      //    executor: this.#executor,
      //    node: CreateTableNode.create(parseTable(table)),
      // })
      console.log('createTable')
      return Object.setPrototypeOf(super.createTable(table), CockroachCreateTableBuilder.prototype)
   }

   withPlugin(plugin: KyselyPlugin): CockroachSchemaModule {
      console.log('schema withPlugin')
      return Object.setPrototypeOf(super.withPlugin(plugin), CockroachSchemaModule.prototype)
   }
   withoutPlugins(): CockroachSchemaModule {
      console.log('schema withoutPlugins')
      return Object.setPrototypeOf(super.withoutPlugins(), CockroachSchemaModule.prototype)
   }
   withSchema(scheme: string): CockroachSchemaModule {
      console.log('schema withSchema')
      return Object.setPrototypeOf(super.withSchema(scheme), CockroachSchemaModule.prototype)
   }
}

class CockroachCreateTableBuilder<TB extends string, C extends string = never> extends CreateTableBuilder<TB, C> {
   addColumn<CN extends string>(columnName: CN, dataType: Arg2<CreateTableBuilder<TB, C>['addColumn']>, build?: CockroachColumnBuilderCallback): CockroachCreateTableBuilder<TB, C | CN> {
      console.log('addColumn', build)
      const shim = build ? (builder: ColumnDefinitionBuilder) => {
         return build(Object.setPrototypeOf(builder, CockroachColumnDefinitionBuilder.prototype))
      } : undefined
      return Object.setPrototypeOf(super.addColumn(columnName, dataType, shim), CockroachCreateTableBuilder.prototype)
  }
}
export declare type CockroachColumnBuilderCallback = (builder: CockroachColumnDefinitionBuilder) => CockroachColumnDefinitionBuilder;

const xassign = (b: ColumnDefinitionBuilder): CockroachColumnDefinitionBuilder => {
   // Object.defineProperties(b, Object.getOwnPropertyDescriptors(Object.getPrototypeOf(CockroachColumnDefinitionBuilder)))
   // return b as CockroachColumnDefinitionBuilder
   // console.log('buildr', b.virtual, CockroachColumnDefinitionBuilder.prototype.virtual)
   return Object.setPrototypeOf(b, CockroachColumnDefinitionBuilder.prototype)
   // Object.entries(Object.getOwnPropertyNames(CockroachColumnDefinitionBuilder)).forEach(([k, v]) => {
   //    Object.assign(b, Object.getOwnPropertyDescriptor() (CockroachColumnDefinitionBuilder)[k])
   // })
   // Object.setPrototypeOf
   // return b as CockroachColumnDefinitionBuilder
}

class CockroachColumnDefinitionBuilder extends ColumnDefinitionBuilder {
   autoIncrement()                                              { return xassign(super.autoIncrement()) }
   primaryKey()                                                 { return xassign(super.primaryKey()) }
   references(ref: string)                                      { return xassign(super.references(ref)) }
   onDelete(onDelete: OnModifyForeignAction)                    { return xassign(super.onDelete(onDelete)) }
   onUpdate(onUpdate: OnModifyForeignAction)                    { return xassign(super.onUpdate(onUpdate)) }
   unique()                                                     { return xassign(super.unique()) }
   notNull()                                                    { return xassign(super.notNull()) }
   unsigned()                                                   { return xassign(super.unsigned()) }
   defaultTo(value: Arg1<ColumnDefinitionBuilder['defaultTo']>) { return xassign(super.defaultTo(value)) }
   check(expression: Expression<any>)                           { return xassign(super.check(expression)) }
   generatedAlwaysAs(expression: Expression<any>)               { return xassign(super.generatedAlwaysAs(expression)) }
   generatedAlwaysAsIdentity()                                  { return xassign(super.generatedAlwaysAsIdentity()) }
   generatedByDefaultAsIdentity()                               { return xassign(super.generatedByDefaultAsIdentity()) }
   modifyFront(modifier: Expression<any>)                       { return xassign(super.modifyFront(modifier)) }
   modifyEnd(modifier: Expression<any>)                         { return xassign(super.modifyEnd(modifier)) }

   stored() {
      if (!this.toOperationNode().generated) {
         throw new Error('stored() can only be called after generatedAlwaysAs');
      }
      if (this.toOperationNode().generated!.virtual) {
         throw new Error('Column cannot be marked as stored if it\'s marked as virtual');
      }
      return new CockroachColumnDefinitionBuilder(ColumnDefinitionNode.cloneWith(this.toOperationNode(), {
         generated: CockroachGeneratedNode.cloneWith(this.toOperationNode().generated!, {
            stored: true,
         }),
      }))
   }
   virtual() {
      if (!this.toOperationNode().generated) {
         throw new Error('stored() can only be called after generatedAlwaysAs');
      }
      if (this.toOperationNode().generated!.stored) {
         throw new Error('Column cannot be marked as virtual if it\'s marked as stored');
      }
      return new CockroachColumnDefinitionBuilder(ColumnDefinitionNode.cloneWith(this.toOperationNode(), {
         generated: CockroachGeneratedNode.cloneWith(this.toOperationNode().generated!, {
            virtual: true,
         }),
      }))
   }

   toOperationNode(): CockroachColumnDefinitionNode {
      return super.toOperationNode();
   }
}

type CockroachGeneratedNodeParams = Omit<CockroachGeneratedNode, 'kind' | 'expression'>
interface CockroachGeneratedNode extends GeneratedNode {
   kind: 'GeneratedNode'
   readonly virtual?: boolean
}

const CockroachGeneratedNode: Readonly<{
   is(node: OperationNode): node is GeneratedNode;
   create(params: CockroachGeneratedNodeParams): GeneratedNode;
   createWithExpression(expression: OperationNode): GeneratedNode;
   cloneWith(node: GeneratedNode, params: CockroachGeneratedNodeParams): GeneratedNode;
}> = GeneratedNode
//    freeze({
//    is(node): node is GeneratedNode {
//       return node.kind === 'GeneratedNode';
//    },
//    create(params) {
//       return freeze({
//          kind: 'GeneratedNode',
//          ...params,
//       });
//    },
//    createWithExpression(expression) {
//       return freeze({
//          kind: 'GeneratedNode',
//          always: true,
//          expression,
//       });
//    },
//    cloneWith(node, params) {
//       return freeze({
//          ...node,
//          ...params,
//       });
//    },
// })
// ==========================
export declare type ColumnDefinitionNodeProps = Omit<Partial<CockroachColumnDefinitionNode>, 'kind' | 'dataType'>;
export interface CockroachColumnDefinitionNode extends ColumnDefinitionNode {
   readonly kind: 'ColumnDefinitionNode';
   readonly generated?: CockroachGeneratedNode;
}

const CockroachColumnDefinitionNode: Readonly<{
   is(node: OperationNode): node is CockroachColumnDefinitionNode;
   create(column: string, dataType: OperationNode): CockroachColumnDefinitionNode;
   cloneWithFrontModifier(node: CockroachColumnDefinitionNode, modifier: OperationNode): CockroachColumnDefinitionNode;
   cloneWithEndModifier(node: CockroachColumnDefinitionNode, modifier: OperationNode): CockroachColumnDefinitionNode;
   cloneWith(node: CockroachColumnDefinitionNode, props: ColumnDefinitionNodeProps): CockroachColumnDefinitionNode;
}> = ColumnDefinitionNode
//    freeze({
//    is(node): node is CockroachColumnDefinitionNode {
//        return node.kind === 'ColumnDefinitionNode';
//    },
//    create(column, dataType) {
//        return freeze({
//            kind: 'ColumnDefinitionNode',
//            column: ColumnNode.create(column),
//            dataType,
//        });
//    },
//    cloneWithFrontModifier(node, modifier) {
//        return freeze({
//            ...node,
//            frontModifiers: node.frontModifiers
//                ? freeze([...node.frontModifiers, modifier])
//                : [modifier],
//        });
//    },
//    cloneWithEndModifier(node, modifier) {
//        return freeze({
//            ...node,
//            endModifiers: node.endModifiers
//                ? freeze([...node.endModifiers, modifier])
//                : [modifier],
//        });
//    },
//    cloneWith(node, props) {
//        return freeze({
//            ...node,
//            ...props,
//        });
//    },
// })

// ============================