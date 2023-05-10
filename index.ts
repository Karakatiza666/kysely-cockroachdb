import { Kysely, DialectAdapter, PostgresDialect, MigrationLockOptions, KyselyPlugin, QueryCompiler, PostgresQueryCompiler, SchemaModule, CreateTableBuilder, QueryExecutor, ColumnBuilderCallback, ColumnDefinitionBuilder, ColumnDefinitionNode, GeneratedNode, OperationNode, ColumnNode, CreateTableBuilderProps } from "kysely"

import { createQueryId } from 'kysely/dist/cjs/util/query-id'
import { CreateTableNode } from 'kysely/dist/cjs/operation-node/create-table-node'
import { parseTable } from 'kysely/dist/cjs/parser/table-parser'
import { DataTypeExpression, parseDataTypeExpression } from "kysely/dist/cjs/parser/data-type-parser"
import { noop, freeze } from 'kysely/dist/cjs/util/object-utils'

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

export class KyselyCockroach<DB> extends Kysely<DB> {
   get schema() {
      return new CockroachSchemaModule(this.getExecutor())
   }
}

class CockroachSchemaModule extends SchemaModule {
   #executor!: QueryExecutor
   constructor(executor: QueryExecutor) {
      super(executor)
      this.#executor = executor
   }
   createTable<TB extends string>(table: TB): CockroachCreateTableBuilder<TB, never> {
      return new CockroachCreateTableBuilder({
         queryId: createQueryId(),
         executor: this.#executor,
         node: CreateTableNode.create(parseTable(table)),
      })
   }
}

class CockroachCreateTableBuilder<TB extends string, C extends string = never> extends CreateTableBuilder<TB, C> {
   #props: CreateTableBuilderProps
   constructor(props: CreateTableBuilderProps) {
      super(props)
      this.#props = freeze(props)
   }
   addColumn<CN extends string>(columnName: CN, dataType: DataTypeExpression, build: ColumnBuilderCallback = noop<ColumnDefinitionBuilder>): CreateTableBuilder<TB, C | CN> {
      const columnBuilder = build(new CockroachColumnDefinitionBuilder(CockroachColumnDefinitionNode.create(columnName, parseDataTypeExpression(dataType))));
      return new CreateTableBuilder({
         ...this.#props,
         node: CreateTableNode.cloneWithColumn(this.#props.node, columnBuilder.toOperationNode()),
      });
   }
}

class CockroachColumnDefinitionBuilder extends ColumnDefinitionBuilder {
   #node: CockroachColumnDefinitionNode
   constructor(node: CockroachColumnDefinitionNode) {
      super(node)
      this.#node = node
   }
   stored(): ColumnDefinitionBuilder {
      if (!this.#node.generated) {
         throw new Error('stored() can only be called after generatedAlwaysAs');
      }
      if (this.#node.generated.virtual) {
         throw new Error('Column cannot be marked as stored if it\'s marked as virtual');
      }
      return new ColumnDefinitionBuilder(ColumnDefinitionNode.cloneWith(this.#node, {
         generated: CockroachGeneratedNode.cloneWith(this.#node.generated, {
            stored: true,
         }),
      }))
   }
   virtual(): ColumnDefinitionBuilder {
      if (!this.#node.generated) {
         throw new Error('stored() can only be called after generatedAlwaysAs');
      }
      if (this.#node.generated.stored) {
         throw new Error('Column cannot be marked as virtual if it\'s marked as stored');
      }
      return new ColumnDefinitionBuilder(ColumnDefinitionNode.cloneWith(this.#node, {
         generated: CockroachGeneratedNode.cloneWith(this.#node.generated, {
            virtual: true,
         }),
      }))
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
}> = freeze({
   is(node): node is GeneratedNode {
      return node.kind === 'GeneratedNode';
   },
   create(params) {
      return freeze({
         kind: 'GeneratedNode',
         ...params,
      });
   },
   createWithExpression(expression) {
      return freeze({
         kind: 'GeneratedNode',
         always: true,
         expression,
      });
   },
   cloneWith(node, params) {
      return freeze({
         ...node,
         ...params,
      });
   },
})
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
}> = freeze({
   is(node): node is CockroachColumnDefinitionNode {
       return node.kind === 'ColumnDefinitionNode';
   },
   create(column, dataType) {
       return freeze({
           kind: 'ColumnDefinitionNode',
           column: ColumnNode.create(column),
           dataType,
       });
   },
   cloneWithFrontModifier(node, modifier) {
       return freeze({
           ...node,
           frontModifiers: node.frontModifiers
               ? freeze([...node.frontModifiers, modifier])
               : [modifier],
       });
   },
   cloneWithEndModifier(node, modifier) {
       return freeze({
           ...node,
           endModifiers: node.endModifiers
               ? freeze([...node.endModifiers, modifier])
               : [modifier],
       });
   },
   cloneWith(node, props) {
       return freeze({
           ...node,
           ...props,
       });
   },
})

// ============================