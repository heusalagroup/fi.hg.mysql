// Copyright (c) 2023. Heusala Group Oy <info@hg.fi>. All rights reserved.

import "../jest/matchers/index";
import { RepositoryUtils } from "../core/data/utils/RepositoryUtils";
import { LogLevel } from "../core/types/LogLevel";
import { CrudRepositoryImpl } from "../core/data/types/CrudRepositoryImpl";
import { allRepositoryTests } from "../core/data/tests/allRepositoryTests";
import { setCrudRepositoryLogLevel } from "../core/data/types/CrudRepository";
import { PersisterMetadataManagerImpl } from "../core/data/persisters/types/PersisterMetadataManagerImpl";
import { MySqlPersister } from "./MySqlPersister";
import { parseNonEmptyString } from "../core/types/String";
import { PersisterType } from "../core/data/persisters/types/PersisterType";

export const TEST_SCOPES             : readonly string[] = (parseNonEmptyString(process?.env?.TEST_SCOPES) ?? '').split(/[,| :;+]+/);
export const MYSQL_HOSTNAME          : string   = parseNonEmptyString(process?.env?.TEST_MYSQL_HOSTNAME)          ?? 'localhost';
export const MYSQL_USERNAME          : string   = parseNonEmptyString(process?.env?.TEST_MYSQL_USERNAME)          ?? 'hg';
export const MYSQL_PASSWORD          : string   = parseNonEmptyString(process?.env?.TEST_MYSQL_PASSWORD)          ?? '';
export const MYSQL_DATABASE          : string   = parseNonEmptyString(process?.env?.TEST_MYSQL_DATABASE)          ?? 'hg';
export const MYSQL_TABLE_PREFIX      : string   = parseNonEmptyString(process?.env?.TEST_MYSQL_TABLE_PREFIX)      ?? 'prefix_';
export const MYSQL_CHARSET           : string   = parseNonEmptyString(process?.env?.TEST_MYSQL_CHARSET )          ?? 'LATIN1_SWEDISH_CI';

export const INTEGRATION_TESTS_ENABLED : boolean = TEST_SCOPES.includes('integration') && !!MYSQL_PASSWORD;

(INTEGRATION_TESTS_ENABLED ? describe : describe.skip)('Repository integrations', () => {

    beforeAll(() => {
        RepositoryUtils.setLogLevel(LogLevel.NONE);
        setCrudRepositoryLogLevel(LogLevel.NONE);
        CrudRepositoryImpl.setLogLevel(LogLevel.NONE);
        PersisterMetadataManagerImpl.setLogLevel(LogLevel.NONE);
        MySqlPersister.setLogLevel(LogLevel.NONE);
    });

    describe('MySQL', () => {
        allRepositoryTests(
            PersisterType.MYSQL,
            () => new MySqlPersister(
                MYSQL_HOSTNAME,
                MYSQL_USERNAME,
                MYSQL_PASSWORD,
                MYSQL_DATABASE,
                MYSQL_TABLE_PREFIX,
                100,
                0,
                60*60*1000,
                60*60*1000,
                60*60*1000,
                60*60*1000,
                true,
                MYSQL_CHARSET
            )
        );
    });

});

