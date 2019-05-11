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

import Symbol, Keyword, SQLServerKeyword, Literals, BaseRule;

grant
    : GRANT (classPrivilegesClause_ | classTypePrivilegesClause_) 
    ;

revoke
    : REVOKE (optionForClause_? classPrivilegesClause_ | classTypePrivilegesClause_)
    ;

deny
    : DENY (classPrivilegesClause_ | classTypePrivilegesClause_)
    ;

classPrivilegesClause_
    : classPrivileges_ (ON onClassClause_)?
    ;

classTypePrivilegesClause_
    : classTypePrivileges_ (ON onClassTypeClause_)?
    ;

optionForClause_
    : GRANT OPTION FOR
    ;

classPrivileges_
    : privilegeType_ columnNames? (COMMA_ privilegeType_ columnNames?)*
    ;

onClassClause_
    : class_? tableName
    ;

classTypePrivileges_
    : privilegeType_ (COMMA_ privilegeType_)*
    ;

onClassTypeClause_
    : classType_? tableName
    ;

privilegeType_
    : ALL PRIVILEGES?
    | ignoredIdentifiers_
    ;

class_
    : IDENTIFIER_ COLON_ COLON_
    ;

classType_
    : (LOGIN | DATABASE | OBJECT | ROLE | SCHEMA | USER) COLON_ COLON_
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

createLogin
    : CREATE LOGIN
    ;

dropLogin
    : DROP LOGIN
    ;

alterLogin
    : ALTER LOGIN
    ;
