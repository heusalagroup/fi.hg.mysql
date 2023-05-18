// Copyright (c) 2022-2023. Heusala Group Oy <info@heusalagroup.fi>. All rights reserved.
// Copyright (c) 2020-2021. Sendanor. All rights reserved.

import { createPool, FieldInfo, MysqlError, Pool } from "mysql";
import { map } from "../core/functions/map";
import { EntityMetadata } from "../core/data/types/EntityMetadata";
import { Persister } from "../core/data/types/Persister";
import { RepositoryError } from "../core/data/types/RepositoryError";
import { RepositoryEntityError } from "../core/data/types/RepositoryEntityError";
import { Entity, EntityIdTypes, isEntity } from "../core/data/Entity";
import { EntityUtils } from "../core/data/utils/EntityUtils";
import { MySqlCharset } from "../core/data/persisters/mysql/types/MySqlCharset";
import { isArray } from "../core/types/Array";
import { LogService } from "../core/LogService";
import { LogLevel } from "../core/types/LogLevel";
import { PersisterMetadataManager } from "../core/data/persisters/types/PersisterMetadataManager";
import { PersisterMetadataManagerImpl } from "../core/data/persisters/types/PersisterMetadataManagerImpl";
import { MySqlEntitySelectQueryBuilder } from "../core/data/query/mysql/select/MySqlEntitySelectQueryBuilder";
import { MySqlAndChainBuilder } from "../core/data/query/mysql/formulas/MySqlAndChainBuilder";
import { Sort } from "../core/data/Sort";
import { Where } from "../core/data/Where";
import { MySqlEntityDeleteQueryBuilder } from "../core/data/query/mysql/delete/MySqlEntityDeleteQueryBuilder";
import { MySqlEntityInsertQueryBuilder } from "../core/data/query/mysql/insert/MySqlEntityInsertQueryBuilder";
import { MySqlEntityUpdateQueryBuilder } from "../core/data/query/mysql/update/MySqlEntityUpdateQueryBuilder";
import { find } from "../core/functions/find";
import { has } from "../core/functions/has";
import { PersisterType } from "../core/data/persisters/types/PersisterType";
import { EntityField } from "../core/data/types/EntityField";
import { TemporalProperty } from "../core/data/types/TemporalProperty";
import { TableFieldInfoCallback, TableFieldInfoResponse } from "../core/data/query/sql/select/EntitySelectQueryBuilder";

export type QueryResultPair = [any, readonly FieldInfo[] | undefined];

const LOG = LogService.createLogger('MySqlPersister');

/**
 * This persister implements entity store over MySQL database.
 *
 * @see {@link Persister}
 */
export class MySqlPersister implements Persister {

    public static setLogLevel (level: LogLevel) {

        LOG.setLogLevel(level);

        // Other internal contexts
        MySqlEntityInsertQueryBuilder.setLogLevel(level);
        EntityUtils.setLogLevel(level);

    }

    private readonly _tablePrefix : string;
    private readonly _queryTimeout : number | undefined;
    private readonly _metadataManager : PersisterMetadataManager;
    private readonly _fetchTableInfo : TableFieldInfoCallback;

    private _pool : Pool | undefined;


    /**
     *
     * @param host
     * @param user
     * @param password
     * @param database
     * @param tablePrefix
     * @param connectionLimit
     * @param queueLimit
     * @param connectTimeout Milliseconds?
     * @param acquireTimeout Seconds -- or Milliseconds?
     * @param timeout Milliseconds
     * @param queryTimeout Milliseconds
     * @param waitForConnections
     * @param charset Connection charset. Defaults to UTF8_GENERAL_CI
     */
    public constructor (
        host: string,
        user: string,
        password: string,
        database: string,
        tablePrefix: string = '',
        connectionLimit: number = 100,
        queueLimit: number = 0,
        acquireTimeout: number = 60*60*1000,
        connectTimeout: number = 60*60*1000,
        timeout : number = 60*60*1000,
        queryTimeout : number | undefined = 60*60*1000,
        waitForConnections : boolean = true,
        charset : MySqlCharset | string = MySqlCharset.UTF8_GENERAL_CI
    ) {
        this._tablePrefix = tablePrefix;
        this._queryTimeout = queryTimeout;
        this._pool = createPool(
            {
                connectionLimit,
                connectTimeout,
                host,
                user,
                charset,
                password,
                database,
                acquireTimeout,
                timeout,
                waitForConnections
            }
        );
        this._metadataManager = new PersisterMetadataManagerImpl();
        this._fetchTableInfo = (tableName: string) : TableFieldInfoResponse => {
            const mappedMetadata = this._metadataManager.getMetadataByTable(tableName);
            if (!mappedMetadata) throw new TypeError(`Could not find metadata for table "${tableName}"`);
            const mappedFields = mappedMetadata.fields;
            const temporalProperties = mappedMetadata.temporalProperties;
            return [mappedFields, temporalProperties];
        };
    }

    /**
     * @inheritDoc
     * @see {@link Persister.getPersisterType}
     */
    public getPersisterType (): PersisterType {
        return PersisterType.MYSQL;
    }

    /**
     * @inheritDoc
     * @see {@link Persister.destroy}
     */
    public destroy () : void {
        if (this._pool) {
            this._pool.end()
            this._pool = undefined;
        }
    }

    /**
     * @inheritDoc
     * @see {@link Persister.setupEntityMetadata}
     * @see {@link PersisterMetadataManager.setupEntityMetadata}
     */
    public setupEntityMetadata (metadata: EntityMetadata) : void {
        this._metadataManager.setupEntityMetadata(metadata);
    }

    /**
     * @inheritDoc
     * @see {@link Persister.count}
     */
    public async count<T extends Entity,
        ID extends EntityIdTypes>(
        metadata : EntityMetadata,
        where    : Where | undefined,
    ): Promise<number> {
        LOG.debug(`count: metadata = `, metadata);
        const {tableName, fields, temporalProperties} = metadata;
        LOG.debug(`count: tableName = `, tableName, fields);
        const builder = MySqlEntitySelectQueryBuilder.create();
        builder.setTablePrefix(this._tablePrefix);
        builder.setTableName(tableName);
        builder.includeFormulaByString('COUNT(*)', 'count');
        if (where !== undefined) {
            builder.setWhereFromQueryBuilder( builder.buildAnd(where, tableName, fields, temporalProperties) )
        }
        const [queryString, queryValues] = builder.build();
        const [results] = await this._query(queryString, queryValues);
        // LOG.debug(`count: results = `, results);
        if (results.length !== 1) {
            throw new RepositoryError(RepositoryError.Code.COUNT_INCORRECT_ROW_AMOUNT, `count: Incorrect amount of rows in the response`);
        }
        return results[0].count;
    }

    /**
     * @inheritDoc
     * @see {@link Persister.existsBy}
     */
    public async existsBy<
        T extends Entity,
        ID extends EntityIdTypes
    >(
        metadata : EntityMetadata,
        where    : Where,
    ): Promise<boolean> {
        LOG.debug(`existsByWhere: where = `, where);
        LOG.debug(`existsByWhere: metadata = `, metadata);
        const { tableName, fields, temporalProperties } = metadata;
        LOG.debug(`count: tableName = `, tableName, fields);
        const builder = MySqlEntitySelectQueryBuilder.create();
        builder.setTablePrefix(this._tablePrefix);
        builder.setTableName(tableName);
        builder.includeFormulaByString('COUNT(*) >= 1', 'exists');
        builder.setWhereFromQueryBuilder( builder.buildAnd(where, tableName, fields, temporalProperties) );
        const [queryString, queryValues] = builder.build();
        const [results] = await this._query(queryString, queryValues);
        if (results.length !== 1) {
            throw new RepositoryError(RepositoryError.Code.EXISTS_INCORRECT_ROW_AMOUNT, `existsById: Incorrect amount of rows in the response`);
        }
        return !!results[0].exists;
    }

    /**
     * @inheritDoc
     * @see {@link Persister.deleteAll}
     */
    public async deleteAll<T extends Entity, ID extends EntityIdTypes>(
        metadata : EntityMetadata,
        where    : Where | undefined,
    ): Promise<void> {
        const { tableName, fields, temporalProperties } = metadata;
        const builder = new MySqlEntityDeleteQueryBuilder();
        builder.setTablePrefix(this._tablePrefix);
        builder.setTableName(tableName);
        if (where) {
            builder.setWhereFromQueryBuilder( builder.buildAnd(where, tableName, fields, temporalProperties) );
        }
        const [queryString, queryValues] = builder.build();
        await this._query(queryString, queryValues);
    }

    /**
     * Find entity using the last insert ID.
     *
     * @param metadata Entity metadata
     * @param sort The sorting criteria
     * @see {@link MySqlPersister.insert}
     */
    public async findByLastInsertId<
        T extends Entity,
        ID extends EntityIdTypes
    >(
        metadata : EntityMetadata,
        sort     : Sort | undefined
    ): Promise<T | undefined> {
        LOG.debug(`findByIdLastInsertId: metadata = `, metadata);

        const {tableName, fields, oneToManyRelations, manyToOneRelations, temporalProperties} = metadata;
        LOG.debug(`findByIdLastInsertId: tableName = `, tableName, fields);
        const mainIdColumnName : string = EntityUtils.getIdColumnName(metadata);
        const builder = MySqlEntitySelectQueryBuilder.create();
        builder.setTablePrefix(this._tablePrefix);
        builder.setTableName(tableName);
        builder.setGroupByColumn(mainIdColumnName);
        builder.includeEntityFields(tableName, fields, temporalProperties);
        if (sort) {
            builder.setOrderByTableFields(sort, tableName, fields);
        }
        builder.setOneToManyRelations(oneToManyRelations, this._fetchTableInfo);
        builder.setManyToOneRelations(manyToOneRelations, this._fetchTableInfo, fields);

        const where = MySqlAndChainBuilder.create();
        where.setColumnEqualsByLastInsertId(builder.getTableNameWithPrefix(tableName), mainIdColumnName);
        builder.setWhereFromQueryBuilder(where);

        const [queryString, queryValues] = builder.build();

        const [results] = await this._query(queryString, queryValues);
        LOG.debug(`findByIdLastInsertId: results = `, results);
        const entity = results.length >= 1 && results[0] ? EntityUtils.toEntity<T, ID>(results[0], metadata, this._metadataManager) : undefined;
        if ( entity !== undefined && !isEntity(entity) ) {
            throw new TypeError(`Could not create entity correctly`);
        }
        return entity;
    }

    /**
     * @inheritDoc
     * @see {@link Persister.findAll}
     */
    public async findAll<T extends Entity,
        ID extends EntityIdTypes>(
        metadata : EntityMetadata,
        where    : Where | undefined,
        sort     : Sort | undefined
    ): Promise<T[]> {
        LOG.debug(`findAll: metadata = `, metadata);
        const {tableName, fields, oneToManyRelations, manyToOneRelations, temporalProperties} = metadata;
        LOG.debug(`findAll: tableName = `, tableName, fields);
        const mainIdColumnName : string = EntityUtils.getIdColumnName(metadata);
        const builder = MySqlEntitySelectQueryBuilder.create();
        builder.setTablePrefix(this._tablePrefix);
        builder.setTableName(tableName);
        if (sort) {
            builder.setOrderByTableFields(sort, tableName, fields);
        }
        builder.setGroupByColumn(mainIdColumnName);
        builder.includeEntityFields(tableName, fields, temporalProperties);
        builder.setOneToManyRelations(oneToManyRelations, this._fetchTableInfo);
        builder.setManyToOneRelations(manyToOneRelations, this._fetchTableInfo, fields);
        if (where) {
            builder.setWhereFromQueryBuilder( builder.buildAnd(where, tableName, fields, temporalProperties) );
        }

        const [queryString, queryValues] = builder.build();

        const [results] = await this._query(queryString, queryValues);
        LOG.debug(`findAll: results = `, results);
        return map(results, (row: any) => EntityUtils.toEntity<T, ID>(row, metadata, this._metadataManager));
    }

    /**
     * @inheritDoc
     * @see {@link Persister.findBy}
     */
    public async findBy<
        T extends Entity,
        ID extends EntityIdTypes
    > (
        metadata : EntityMetadata,
        where    : Where,
        sort     : Sort | undefined
    ): Promise<T | undefined> {
        const {tableName, fields, oneToManyRelations, manyToOneRelations, temporalProperties} = metadata;
        const mainIdColumnName : string = EntityUtils.getIdColumnName(metadata);
        const builder = MySqlEntitySelectQueryBuilder.create();
        builder.setTablePrefix(this._tablePrefix);
        builder.setTableName(tableName);
        if (sort) {
            builder.setOrderByTableFields(sort, tableName, fields);
        }
        builder.setGroupByColumn(mainIdColumnName);
        builder.includeEntityFields(tableName, fields, temporalProperties);
        builder.setOneToManyRelations(oneToManyRelations, this._fetchTableInfo);
        builder.setManyToOneRelations(manyToOneRelations, this._fetchTableInfo, fields);
        if (where !== undefined) {
            builder.setWhereFromQueryBuilder( builder.buildAnd(where, tableName, fields, temporalProperties) )
        }
        const [queryString, queryValues] = builder.build();
        const [results] = await this._query(queryString, queryValues);
        return results.length >= 1 && results[0] ? EntityUtils.toEntity<T, ID>(results[0], metadata, this._metadataManager) : undefined;
    }

    /**
     * @inheritDoc
     * @see {@link Persister.insert}
     */
    public async insert<T extends Entity, ID extends EntityIdTypes>(
        metadata: EntityMetadata,
        entities: T | readonly T[],
    ): Promise<T> {
        LOG.debug(`insert: entities = `, entities, metadata);
        if ( !isArray(entities) ) {
            entities = [entities];
        }
        if ( entities?.length < 1 ) {
            throw new TypeError(`No entities provided. You need to provide at least one entity to insert.`);
        }
        // Make sure all of our entities have the same metadata
        if (!EntityUtils.areEntitiesSameType(entities)) {
            throw new TypeError(`Insert can only insert entities of the same time. There were some entities with different metadata than provided.`);
        }
        const { tableName, fields, temporalProperties, idPropertyName } = metadata;

        const builder = MySqlEntityInsertQueryBuilder.create();
        builder.setTablePrefix(this._tablePrefix);
        builder.setTableName(tableName);

        builder.appendEntityList(
            entities,
            fields,
            temporalProperties,
            [idPropertyName]
        );

        const [ queryString, params ] = builder.build();
        const [ results ] = await this._query(queryString, params);
        // Note! We cannot use `results?.insertId` since it is numeric even for BIGINT types and so is not safe. We'll just log it here for debugging purposes.
        const entityId = results?.insertId;
        if (!entityId) {
            throw new RepositoryError(RepositoryError.Code.CREATED_ENTITY_ID_NOT_FOUND, `Entity id could not be found for newly created entity in table ${tableName}`);
        }
        const resultEntity: T | undefined = await this.findByLastInsertId(metadata, Sort.by(metadata.idPropertyName));
        if ( !resultEntity ) {
            throw new RepositoryEntityError(entityId, RepositoryEntityError.Code.ENTITY_NOT_FOUND, `Newly created entity not found in table ${tableName}: #${entityId}`);
        }
        return resultEntity;
    }

    /**
     * @inheritDoc
     * @see {@link Persister.update}
     */
    public async update<T extends Entity, ID extends EntityIdTypes>(
        metadata: EntityMetadata,
        entity: T,
    ): Promise<T> {
        const { tableName, fields, temporalProperties, idPropertyName } = metadata;

        const idField = find(fields, item => item.propertyName === idPropertyName);
        if (!idField) throw new TypeError(`Could not find id field using property "${idPropertyName}"`);
        const idColumnName = idField.columnName;
        if (!idColumnName) throw new TypeError(`Could not find id column using property "${idPropertyName}"`);

        const entityId = has(entity,idPropertyName) ? (entity as any)[idPropertyName] : undefined;
        if (!entityId) throw new TypeError(`Could not find entity id column using property "${idPropertyName}"`);

        const builder = MySqlEntityUpdateQueryBuilder.create();
        builder.setTablePrefix(this._tablePrefix);
        builder.setTableName(tableName);

        builder.appendEntity(
            entity,
            fields,
            temporalProperties,
            [idPropertyName]
        );

        const where = MySqlAndChainBuilder.create();
        where.setColumnEquals(this._tablePrefix+tableName, idColumnName, entityId);
        builder.setWhereFromQueryBuilder(where);

        // builder.setEntities(metadata, entities);
        const [ queryString, queryValues ] = builder.build();

        await this._query(queryString, queryValues);

        const resultEntity: T | undefined = await this.findBy(metadata, Where.propertyEquals(idPropertyName, entityId), Sort.by(metadata.idPropertyName));
        if (resultEntity) {
            return resultEntity;
        } else {
            throw new RepositoryEntityError(entityId, RepositoryEntityError.Code.ENTITY_NOT_FOUND, `Entity not found: #${entityId}`);
        }
    }

    /**
     * Performs the actual SQL query.
     *
     * @param query The query string with parameter placeholders
     * @param values The values for parameter placeholders
     * @private
     */
    private async _query(
        query: string,
        values ?: readonly any[]
    ): Promise<QueryResultPair> {
        LOG.debug(`Query "${query}" with values: `, values);
        const pool = this._pool;
        if (!pool) throw new TypeError(`This persister has been destroyed`);
        try {
            return await new Promise((resolve, reject) => {
                try {
                    pool.query(
                        {
                            sql: query,
                            values: values,
                            timeout: this._queryTimeout
                        },
                        (error: MysqlError | null, results ?: any, fields?: FieldInfo[]) => {
                            if (error) {
                                reject(error);
                            } else {
                                resolve([results, fields]);
                            }
                        }
                    );
                } catch (err) {
                    reject(err);
                }
            });
        } catch (err) {
            LOG.debug(`Query failed: `, query, values);
            throw TypeError(`Query failed: "${query}": ${err}`);
        }
    }

}
