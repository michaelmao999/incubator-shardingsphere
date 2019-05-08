package org.apache.shardingsphere.core.parse.old.parser.clause.facade;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.parse.old.parser.clause.*;

/**
 * MERGE INTO target_table
 * USING source_table
 * ON search_condition
 *     WHEN MATCHED THEN
 *         UPDATE SET col1 = value1, col2 = value2,...
 *         WHERE <update_condition>
 *         [DELETE WHERE <delete_condition>]
 *     WHEN NOT MATCHED THEN
 *         INSERT (col1,col2,...)
 *         values(value1,value2,...)
 *         WHERE <insert_condition>;
 */

@RequiredArgsConstructor
@Getter
public abstract class AbstractMergeClauseParserFacade {

    private final TableReferencesClauseParser tableReferencesClauseParser;

    private final SelectListClauseParser usingSelectListClauseParser;

    private final TableReferencesClauseParser usingTableReferencesClauseParser;

    private final WhereClauseParser usingWhereClauseParser;

    private final GroupByClauseParser usingGroupByClauseParser;

    private final HavingClauseParser usingHavingClauseParser;

    private final OrderByClauseParser usingOrderByClauseParser;

    private final SelectRestClauseParser usingSelectRestClauseParser;

    private final TableReferencesClauseParser updateTableReferencesClauseParser;

    private final UpdateSetItemsClauseParser updateSetItemsClauseParser;

    private final WhereClauseParser updateWhereClauseParser;

    private final TableReferencesClauseParser deleteTableReferencesClauseParser;

    private final WhereClauseParser deleteWhereClauseParser;

    private final InsertIntoClauseParser insertIntoClauseParser;

    private final InsertColumnsClauseParser insertColumnsClauseParser;

    private final InsertValuesClauseParser insertValuesClauseParser;

    private final InsertSetClauseParser insertSetClauseParser;

    private final InsertDuplicateKeyUpdateClauseParser insertDuplicateKeyUpdateClauseParser;

}
