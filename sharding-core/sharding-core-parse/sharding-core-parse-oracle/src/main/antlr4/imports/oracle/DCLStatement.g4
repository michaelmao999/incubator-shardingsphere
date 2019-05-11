/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar DCLStatement;

import Symbol, Keyword, OracleKeyword, Literals, BaseRule;

grant
    : GRANT (objectPrivilegeClause_ | systemPrivilegeClause_ | roleClause_) 
    ;

revoke
    : REVOKE (objectPrivilegeClause_ | systemPrivilegeClause_ | roleClause_)
    ;

objectPrivilegeClause_
    : objectPrivileges_ ON onObjectClause_
    ;

systemPrivilegeClause_
    : systemPrivilege_
    ;
    
roleClause_
    : ignoredIdentifiers_
    ;

objectPrivileges_
    : objectPrivilegeType_ columnNames? (COMMA_ objectPrivilegeType_ columnNames?)*
    ;

objectPrivilegeType_
    : ALL PRIVILEGES?
    | SELECT
    | INSERT
    | DELETE
    | UPDATE
    | ALTER
    | READ
    | WRITE
    | EXECUTE
    | USE
    | INDEX
    | REFERENCES
    | DEBUG
    | UNDER
    | FLASHBACK ARCHIVE
    | ON COMMIT REFRESH
    | QUERY REWRITE
    | KEEP SEQUENCE
    | INHERIT PRIVILEGES
    | TRANSLATE SQL
    | MERGE VIEW
    ;

onObjectClause_
    : USER | DIRECTORY | EDITION | MINING MODEL | SQL TRANSLATION PROFILE
    | JAVA (SOURCE | RESOURCE) tableName
    | tableName
    ;

systemPrivilege_
    : ALL PRIVILEGES
    | ignoredIdentifiers_
    ;

createUser
    : CREATE USER
    ;

dropUser
    : DROP USER
    ;

alterUser
    : ALTER USER
    ;

createRole
    : CREATE ROLE
    ;

dropRole
    : DROP ROLE
    ;

alterRole
    : ALTER ROLE
    ;
