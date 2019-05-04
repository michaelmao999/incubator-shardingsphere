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

lexer grammar Keyword;

import Alphabet;

WS
    : [ \t\r\n] + ->skip
    ;

ALL
    : A L L
    ;

AND
    : A N D
    ;

ANY
    : A N Y
    ;

ASC
    : A S C
    ;

BETWEEN
    : B E T W E E N
    ;

BINARY
    : B I N A R Y
    ;

BY
    : B Y
    ;

DATE
    : D A T E
    ;

DESC
    : D E S C
    ;

DISTINCT
    : D I S T I N C T
    ;

ESCAPE
    : E S C A P E
    ;

EXISTS
    : E X I S T S
    ;

FALSE
    : F A L S E
    ;

FROM
    : F R O M
    ;

GROUP
    : G R O U P
    ;

HAVING
    : H A V I N G
    ;

IN
    : I N
    ;

IS
    : I S
    ;

KEY
    : K E Y
    ;

LIKE
    : L I K E
    ;

LIMIT
    : L I M I T
    ;

MOD
    : M O D
    ;

DIV
    : D I V
    ;

NOT
    : N O T
    ;

NULL
    : N U L L
    ;

OFFSET
    : O F F S E T
    ;

OR
    : O R
    ;

ORDER
    : O R D E R
    ;

PARTITION
    : P A R T I T I O N
    ;

PRIMARY
    : P R I M A R Y
    ;

REGEXP
    : R E G E X P
    ;

RLIKE
    : R L I K E
    ;

ROLLUP
    : R O L L U P
    ;

ROW
    : R O W
    ;

SET
    : S E T
    ;

SOUNDS
    : S O U N D S
    ;

TIME
    : T I M E
    ;

TIMESTAMP
    : T I M E S T A M P
    ;

TRUE
    : T R U E
    ;

UNION
    : U N I O N
    ;

UNKNOWN
    : U N K N O W N
    ;

WHERE
    : W H E R E
    ;

WITH
    : W I T H
    ;

XOR
    : X O R
    ;

ADD
    : A D D
    ;

ALTER
    : A L T E R
    ;

ALWAYS
    : A L W A Y S
    ;

AS
    : A S
    ;

CASCADE
    : C A S C A D E
    ;

CHECK
    : C H E C K
    ;

COLUMN
    : C O L U M N
    ;

COMMIT
    : C O M M I T
    ;

COMMITTED
    : C O M M I T T E D
    ;

CONSTRAINT
    : C O N S T R A I N T
    ;

CREATE
    : C R E A T E
    ;

CURRENT
    : C U R R E N T
    ;

DEFAULT
    : D E F A U L T
    ;

DELETE
    : D E L E T E
    ;

DISABLE
    : D I S A B L E
    ;

DROP
    : D R O P
    ;

ENABLE
    : E N A B L E
    ;

FOR
    : F O R
    ;

FOREIGN
    : F O R E I G N
    ;

FUNCTION
    : F U N C T I O N
    ;

GENERATED
    : G E N E R A T E D
    ;

GRANT
    : G R A N T
    ;

INDEX
    : I N D E X
    ;

LEVEL
    : L E V E L
    ;

NO
    : N O
    ;

ON
    : O N
    ;

OPTION
    : O P T I O N
    ;

PASSWORD
    : P A S S W O R D
    ;

PRIVILEGES
    : P R I V I L E G E S
    ;

READ
    : R E A D
    ;

REFERENCES
    : R E F E R E N C E S
    ;

REVOKE
    : R E V O K E
    ;

ROLE
    : R O L E
    ;

ROLLBACK
    : R O L L B A C K
    ;

ROWS
    : R O W S
    ;

START
    : S T A R T
    ;

TABLE
    : T A B L E
    ;

TO
    : T O
    ;

TRANSACTION
    : T R A N S A C T I O N
    ;

TRUNCATE
    : T R U N C A T E
    ;

UNIQUE
    : U N I Q U E
    ;

USER
    : U S E R
    ;

ACCOUNT
    : A C C O U N T
    ;

ACTION
    : A C T I O N
    ;

AFTER
    : A F T E R
    ;

ALGORITHM
    : A L G O R I T H M
    ;

ANALYZE
    : A N A L Y Z E
    ;

AUDIT_ADMIN
    : A U D I T UL_ A D M I N
    ;

AUTO_INCREMENT
    : A U T O UL_ I N C R E M E N T
    ;

AUTOCOMMIT
    : A U T O C O M M I T
    ;

AVG_ROW_LENGTH
    : A V G UL_ R O W UL_ L E N G T H
    ;

BEGIN
    : B E G I N
    ;

BINLOG_ADMIN
    : B I N L O G UL_ A D M I N
    ;

BOTH
    : B O T H
    ;

BTREE
    : B T R E E
    ;

CASE
    : C A S E
    ;

CAST
    : C A S T
    ;

CHAIN
    : C H A I N
    ;

CHANGE
    : C H A N G E
    ;

CHAR
    : C H A R
    ;

CHARACTER
    : C H A R A C T E R
    ;

CHARSET
    : C H A R S E T
    ;

CHECKSUM
    : C H E C K S U M
    ;

CIPHER
    : C I P H E R
    ;

CLIENT
    : C L I E N T
    ;

COALESCE
    : C O A L E S C E
    ;

COLLATE
    : C O L L A T E
    ;

COLUMNS
    : C O L U M N S
    ;

COLUMN_FORMAT
    : C O L U M N UL_ F O R M A T
    ;

COMMENT
    : C O M M E N T
    ;

COMPACT
    : C O M P A C T
    ;

COMPRESSED
    : C O M P R E S S E D
    ;

COMPRESSION
    : C O M P R E S S I O N
    ;

CONNECTION
    : C O N N E C T I O N
    ;

CONNECTION_ADMIN
    : C O N N E C T I O N UL_ A D M I N
    ;

CONSISTENT
    : C O N S I S T E N T
    ;

CONVERT
    : C O N V E R T
    ;

COPY
    : C O P Y
    ;

CROSS
    : C R O S S
    ;

CURRENT_TIMESTAMP
    : C U R R E N T UL_ T I M E S T A M P
    ;

DATA
    : D A T A
    ;

DATABASES
    : D A T A B A S E S
    ;

DELAYED
    : D E L A Y E D
    ;

DELAY_KEY_WRITE
    : D E L A Y UL_ K E Y UL_ W R I T E
    ;

DIRECTORY
    : D I R E C T O R Y
    ;

DISCARD
    : D I S C A R D
    ;

DISK
    : D I S K
    ;

DISTINCTROW
    : D I S T I N C T R O W
    ;

DOUBLE
    : D O U B L E
    ;

DUPLICATE
    : D U P L I C A T E
    ;

DYNAMIC
    : D Y N A M I C
    ;

ELSE
    : E L S E
    ;

ENCRYPTION
    : E N C R Y P T I O N
    ;

ENCRYPTION_KEY_ADMIN
    : E N C R Y P T I O N UL_ K E Y UL_ A D M I N
    ;

END
    : E N D
    ;

ENGINE
    : E N G I N E
    ;

EVENT
    : E V E N T
    ;

EXCEPT
    : E X C E P T
    ;

EXCHANGE
    : E X C H A N G E
    ;

EXCLUSIVE
    : E X C L U S I V E
    ;

EXECUTE
    : E X E C U T E
    ;

EXTRACT
    : E X T R A C T
    ;

FILE
    : F I L E
    ;

FIREWALL_ADMIN
    : F I R E W A L L UL_ A D M I N
    ;

FIREWALL_USER
    : F I R E W A L L UL_ U S E R
    ;

FIRST
    : F I R S T
    ;

FIXED
    : F I X E D
    ;

FOLLOWING
    : F O L L O W I N G
    ;

FORCE
    : F O R C E
    ;

FULL
    : F U L L
    ;

FULLTEXT
    : F U L L T E X T
    ;

GLOBAL
    : G L O B A L
    ;

GROUP_REPLICATION_ADMIN
    : G R O U P UL_ R E P L I C A T I O N UL_ A D M I N
    ;

HASH
    : H A S H
    ;

HIGH_PRIORITY
    : H I G H UL_ P R I O R I T Y
    ;

IDENTIFIED
    : I D E N T I F I E D
    ;

IF
    : I F
    ;

IGNORE
    : I G N O R E
    ;

IMPORT_
    : I M P O R T UL_
    ;

INNER
    : I N N E R
    ;

INPLACE
    : I N P L A C E
    ;

INSERT
    : I N S E R T
    ;

INSERT_METHOD
    : I N S E R T UL_ M E T H O D
    ;

INTERVAL
    : I N T E R V A L
    ;

INTO
    : I N T O
    ;

JOIN
    : J O I N
    ;

KEYS
    : K E Y S
    ;

KEY_BLOCK_SIZE
    : K E Y UL_ B L O C K UL_ S I Z E
    ;

LAST
    : L A S T
    ;

LEADING
    : L E A D I N G
    ;

LEFT
    : L E F T
    ;

LESS
    : L E S S
    ;

LINEAR
    : L I N E A R
    ;

LOCALTIME
    : L O C A L T I M E
    ;

LOCALTIMESTAMP
    : L O C A L T I M E S T A M P
    ;

LOCK
    : L O C K
    ;

LOW_PRIORITY
    : L O W UL_ P R I O R I T Y
    ;

MATCH
    : M A T C H
    ;

MAXVALUE
    : M A X V A L U E
    ;

MAX_ROWS
    : M A X UL_ R O W S
    ;

MEMORY
    : M E M O R Y
    ;

MIN_ROWS
    : M I N UL_ R O W S
    ;

MODIFY
    : M O D I F Y
    ;

NATURAL
    : N A T U R A L
    ;

NONE
    : N O N E
    ;

NOW
    : N O W
    ;

OFFLINE
    : O F F L I N E
    ;

ONLINE
    : O N L I N E
    ;

OPTIMIZE
    : O P T I M I Z E
    ;

OUTER
    : O U T E R
    ;

OVER
    : O V E R
    ;

PACK_KEYS
    : P A C K UL_ K E Y S
    ;

PARSER
    : P A R S E R
    ;

PARTIAL
    : P A R T I A L
    ;

PARTITIONING
    : P A R T I T I O N I N G
    ;

PERSIST
    : P E R S I S T
    ;

PERSIST_ONLY
    : P E R S I S T UL_ O N L Y
    ;

POSITION
    : P O S I T I O N
    ;

PRECEDING
    : P R E C E D I N G
    ;

PRECISION
    : P R E C I S I O N
    ;

PROCEDURE
    : P R O C E D U R E
    ;

PROCESS
    : P R O C E S S
    ;

PROXY
    : P R O X Y
    ;

QUICK
    : Q U I C K
    ;

RANGE
    : R A N G E
    ;

REBUILD
    : R E B U I L D
    ;

RECURSIVE
    : R E C U R S I V E
    ;

REDUNDANT
    : R E D U N D A N T
    ;

RELEASE
    : R E L E A S E
    ;

RELOAD
    : R E L O A D
    ;

REMOVE
    : R E M O V E
    ;

RENAME
    : R E N A M E
    ;

REORGANIZE
    : R E O R G A N I Z E
    ;

REPAIR
    : R E P A I R
    ;

REPLACE
    : R E P L A C E
    ;

REPLICATION
    : R E P L I C A T I O N
    ;

REPLICATION_SLAVE_ADMIN
    : R E P L I C A T I O N UL_ S L A V E UL_ A D M I N
    ;

REQUIRE
    : R E Q U I R E
    ;

RESTRICT
    : R E S T R I C T
    ;

REVERSE
    : R E V E R S E
    ;

RIGHT
    : R I G H T
    ;

ROLE_ADMIN
    : R O L E UL_ A D M I N
    ;

ROUTINE
    : R O U T I N E
    ;

ROW_FORMAT
    : R O W UL_ F O R M A T
    ;

SAVEPOINT
    : S A V E P O I N T
    ;

SELECT
    : S E L E C T
    ;

SEPARATOR
    : S E P A R A T O R
    ;

SESSION
    : S E S S I O N
    ;

SET_USER_ID
    : S E T UL_ U S E R UL_ I D
    ;

SHARED
    : S H A R E D
    ;

SHOW
    : S H O W
    ;

SHUTDOWN
    : S H U T D O W N
    ;

SIMPLE
    : S I M P L E
    ;

SLAVE
    : S L A V E
    ;

SPATIAL
    : S P A T I A L
    ;

SQLDML
    : S Q L D M L
    ;

SQLDQL
    : S Q L D Q L
    ;

SQL_BIG_RESULT
    : S Q L UL_ B I G UL_ R E S U L T
    ;

SQL_BUFFER_RESULT
    : S Q L UL_ B U F F E R UL_ R E S U L T
    ;

SQL_CACHE
    : S Q L UL_ C A C H E
    ;

SQL_CALC_FOUND_ROWS
    : S Q L UL_ C A L C UL_ F O U N D UL_ R O W S
    ;

SQL_NO_CACHE
    : S Q L UL_ N O UL_ C A C H E
    ;

SQL_SMALL_RESULT
    : S Q L UL_ S M A L L UL_ R E S U L T
    ;

SSL
    : S S L
    ;

STATS_AUTO_RECALC
    : S T A T S UL_ A U T O UL_ R E C A L C
    ;

STATS_PERSISTENT
    : S T A T S UL_ P E R S I S T E N T
    ;

STATS_SAMPLE_PAGES
    : S T A T S UL_ S A M P L E UL_ P A G E S
    ;

STORAGE
    : S T O R A G E
    ;

STORED
    : S T O R E D
    ;

STRAIGHT_JOIN
    : S T R A I G H T UL_ J O I N
    ;

SUBPARTITION
    : S U B P A R T I T I O N
    ;

SUPER
    : S U P E R
    ;

SUBSTR
    : S U B S T R
    ;

SUBSTRING
    : S U B S T R I N G
    ;

SYSTEM_VARIABLES_ADMIN
    : S Y S T E M UL_ V A R I A B L E S UL_ A D M I N
    ;

TABLES
    : T A B L E S
    ;

TABLESPACE
    : T A B L E S P A C E
    ;

TEMPORARY
    : T E M P O R A R Y
    ;

THAN
    : T H A N
    ;

THEN
    : T H E N
    ;

TRAILING
    : T R A I L I N G
    ;

TRIGGER
    : T R I G G E R
    ;

TRIM
    : T R I M
    ;

UNBOUNDED
    : U N B O U N D E D
    ;

UNLOCK
    : U N L O C K
    ;

UNSIGNED
    : U N S I G N E D
    ;

UPDATE
    : U P D A T E
    ;

UPGRADE
    : U P G R A D E
    ;

USAGE
    : U S A G E
    ;

USE
    : U S E
    ;

USING
    : U S I N G
    ;

VALIDATION
    : V A L I D A T I O N
    ;

VALUE
    : V A L U E
    ;

VALUES
    : V A L U E S
    ;

VERSION_TOKEN_ADMIN
    : V E R S I O N UL_ T O K E N UL_ A D M I N
    ;

VIEW
    : V I E W
    ;

VIRTUAL
    : V I R T U A L
    ;

WEIGHT_STRING
    : W E I G H T UL_ S T R I N G
    ;

WHEN
    : W H E N
    ;

WINDOW
    : W I N D O W
    ;

WITHOUT
    : W I T H O U T
    ;

WRITE
    : W R I T E
    ;

ZEROFILL
    : Z E R O F I L L
    ;

VISIBLE
    : V I S I B L E
    ;

INVISIBLE
    : I N V I S I B L E
    ;

INSTANT
    : I N S T A N T
    ;

ENFORCED
    : E N F O R C E D
    ;

OJ
    : O J
    ;

MICROSECOND
    : M I C R O S E C O N D
    ;

SECOND
    : S E C O N D
    ;
   
MINUTE
    : M I N U T E
    ;
   
HOUR
    : H O U R
    ;

DAY
    : D A Y
    ;

WEEK
    : W E E K
    ;

MONTH
    : M O N T H
    ;

QUARTER
    : Q U A R T E R
    ;

YEAR
    : Y E A R
    ;

SECOND_MICROSECOND
    : S E C O N D UL_ M I C R O S E C O N D
    ;

MINUTE_MICROSECOND
    : M I N U T E UL_ M I C R O S E C O N D
    ;

MINUTE_SECOND
    : M I N U T E UL_ S E C O N D
    ;

HOUR_MICROSECOND
    : H O U R UL_ M I C R O S E C O N D
    ;

HOUR_SECOND
    : H O U R UL_ S E C O N D
    ;

HOUR_MINUTE
    : H O U R UL_ M I N U T E
    ;

DAY_MICROSECOND
    : D A Y UL_ M I C R O S E C O N D
    ;

DAY_SECOND
    : D A Y UL_ S E C O N D
    ;

DAY_MINUTE
    : D A Y UL_ M I N U T E
    ;

DAY_HOUR
    : D A Y UL_ H O U R
    ;

YEAR_MONTH
    : D A Y UL_ M O N T H
    ;

AGAINST
    : A G A I N S T
    ;

LANGUAGE
    : L A N G U A G E
    ;

MODE
    : M O D E
    ;

QUERY
    : Q U E R Y
    ;

EXPANSION
    : E X P A N S I O N
    ;

BOOLEAN
    : B O O L E A N
    ;

MAX
    : M A X
    ;

MIN
    : M I N
    ;
    
SUM
    : S U M
    ;

COUNT
    : C O U N T
    ;

AVG
    : A V G
    ;

BIT_AND
    : B I T UL_ A N D
    ;

BIT_OR
    : B I T UL_ O R
    ;

BIT_XOR
    : B I T UL_ X O R
    ;

GROUP_CONCAT
    : G R O U P UL_ C O N C A T
    ;

JSON_ARRAYAGG
    : J S O N UL_ A R R A Y A G G
    ;

JSON_OBJECTAGG
    : J S O N UL_ O B J E C T A G G
    ;

STD
    : S T D
    ;

STDDEV
    : S T D D E V
    ;

STDDEV_POP
    : S T D D E V UL_ P O P
    ;

STDDEV_SAMP
    : S T D D E V UL_ S A M P
    ;

VAR_POP
    : V A R UL_ P O P
    ;

VAR_SAMP
    : V A R UL_ S A M P
    ;

VARIANCE
    : V A R I A N C E
    ;

DESCRIBE
    : D E S C R I B E
    ;
