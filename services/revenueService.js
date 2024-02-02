class revenueService {

    getData(connection, request, resultsCallback) {
        const SQL = this.buildSql(request);
        //console.log(SQL);
        connection.query(SQL, (error, results) => {
            if (error) {
                //errorlogger.error(error.message);
            }

            const rowCount = this.getRowCount(request, results);
            const resultsForPage = this.cutResultsToPageSize(request, results);
            const currentSql = SQL;
            resultsCallback(resultsForPage, rowCount, currentSql);
        });
    }

    getDataCount(connection, request, resultsCallback) {
        const SQL = this.buildSql(request, "show");
        connection.query(SQL, (error, results) => {
            resultsCallback(results[0]["TOTAL_RECORDS"]);
        });
    }

    buildSql(request, totalrecords = "") {

        const selectSql = this.createSelectSql(request);
        var category_join = "";
        if ((request.cats).length > 0) {
            category_join = "INNER JOIN price_management_catpro AS pmcp ON pmcp.product_id = rd.product_id "
        }

        const fromSql = " FROM gyzsrevenuedata AS rd LEFT JOIN price_management_data AS pmd ON pmd.product_id = rd.product_id " + category_join + "";
        const whereSql = this.createWhereSql(request);
        const limitSql = this.createLimitSql(request);

        const orderBySql = this.createOrderBySql(request);
        const groupBySql = this.createGroupBySql(request);

        if (totalrecords == "show") {
            var SQLCOUNT = this.buildsqlcount(fromSql, whereSql);
            return SQLCOUNT;
        } else {
            const SQL = selectSql + fromSql + whereSql + groupBySql + orderBySql + limitSql;
            return SQL;
        }
    }

    buildsqlcount(fromSql, whereSql) {
        var countsql = "";
        return countsql = "SELECT COUNT(DISTINCT rd.product_id) AS TOTAL_RECORDS " + fromSql + " " + whereSql + "";
    }

    getAllRevenue(connection, request, resultsCallback) {
        connection.query(request, (error, results) => {
            resultsCallback(results);
        });
    }

    createSelectSql(request) {
        const rowGroupCols = request.rowGroupCols;
        const valueCols = request.valueCols;
        const groupKeys = request.groupKeys;

        if (this.isDoingGrouping(rowGroupCols, groupKeys)) {
            const colsToSelect = [];

            const rowGroupCol = rowGroupCols[groupKeys.length];
            colsToSelect.push(rowGroupCol.field);

            valueCols.forEach(function (valueCol) {
                colsToSelect.push(valueCol.aggFunc + '(' + valueCol.field + ') as ' + valueCol.field);
            });

            return ' select ' + colsToSelect.join(', ');
        }

        return ' select DISTINCT rd.*, pmd.supplier_type, pmd.name, pmd.merk,pmd.categories, pmd.supplier_type';
    }

    createFilterSql(key, item) {
        // console.log(key);
        // console.log(item);
        switch (item.filterType) {
            case 'text':
                return this.createTextFilterSql(key, item);
            case 'number':
                return this.createNumberFilterSql(key, item);
            case 'set':
                return this.createSetFilterSql(key, item);
            default:
                console.log('unkonwn filter type: ' + item.filterType);
        }
    }

    createSetFilterSql(key, item) {
        var allValues = (item.values).join('","');
        return key + ' IN ("' + allValues + '")';
    }

    createNumberFilterSql(key, item) {
        var ptable = ["supplier_type", "name", "merk"];
        var col_prefix = "rd.";
        if (ptable.includes(key)) {
            var col_prefix = "pmd.";
        }

        switch (item.type) {
            case 'equals':
                return col_prefix + key + ' = ' + item.filter;
            case 'notEqual':
                return col_prefix + key + ' != ' + item.filter;
            case 'greaterThan':
                return col_prefix + key + ' > ' + item.filter;
            case 'greaterThanOrEqual':
                return col_prefix + key + ' >= ' + item.filter;
            case 'lessThan':
                return col_prefix + key + ' < ' + item.filter;
            case 'lessThanOrEqual':
                return col_prefix + key + ' <= ' + item.filter;
            case 'inRange':
                return '(' + col_prefix + key + ' >= ' + item.filter + ' and ' + col_prefix + key + ' <= ' + item.filterTo + ')';
            default:
                console.log('unknown number filter type: ' + item.type);
                return 'true';
        }
    }

    createTextFilterSql(key, item) {
        var ptable = ["supplier_type", "name", "merk"];
        var col_prefix = "rd.";
        if (ptable.includes(key)) {
            var col_prefix = "pmd.";
        }
        switch (item.type) {
            case 'equals':
                return col_prefix + key + ' = "' + item.filter + '"';
            case 'notEqual':
                return col_prefix + key + ' != "' + item.filter + '"';
            case 'contains':
                return col_prefix + key + ' like "%' + item.filter + '%"';
            case 'notContains':
                return col_prefix + key + ' not like "%' + item.filter + '%"';
            case 'startsWith':
                return col_prefix + key + ' like "' + item.filter + '%"';
            case 'endsWith':
                return col_prefix + key + ' like "%' + item.filter + '"';
            default:
                console.log('unknown text filter type: ' + item.type);
                return 'true';
        }
    }

    createWhereSql(request) {
        const rowGroupCols = request.rowGroupCols;
        const groupKeys = request.groupKeys;
        const filterModel = request.filterModel;

        const that = this;
        const whereParts = [];

        if (groupKeys.length > 0) {
            groupKeys.forEach(function (key, index) {
                const colName = rowGroupCols[index].field;
                whereParts.push(colName + ' = "' + key + '"')
            });
        }

        if (filterModel) {
            const keySet = Object.keys(filterModel);
            keySet.forEach(function (key) {
                const item = filterModel[key];
                whereParts.push(that.createFilterSql(key, item));
            });
        }

        /* if (whereParts.length > 0) {
            return ' where ' + whereParts.join(' and ');
        } else {
            return '';
        } */
        var whereClause = "";

        if (whereParts.length > 0) {
            whereClause = whereParts.join(' AND ');
            whereClause += ' AND';
        }
        if ((request.cats).length > 0) {
            whereClause += ' pmcp.category_id IN (' + request.cats + ') AND';
        }
        if (whereClause == "") {
            return '';
        } else {
            return 'where ' + whereClause.replace(/ AND$/, '') + '';
        }
    }

    createGroupBySql(request) {
        const rowGroupCols = request.rowGroupCols;
        const groupKeys = request.groupKeys;

        if (this.isDoingGrouping(rowGroupCols, groupKeys)) {
            const colsToGroupBy = [];

            const rowGroupCol = rowGroupCols[groupKeys.length];
            colsToGroupBy.push(rowGroupCol.field);

            return ' group by ' + colsToGroupBy.join(', ');
        } else {
            // select all columns
            return '';
        }
    }

    createOrderBySql(request) {
        const rowGroupCols = request.rowGroupCols;
        const groupKeys = request.groupKeys;
        const sortModel = request.sortModel;

        const grouping = this.isDoingGrouping(rowGroupCols, groupKeys);

        const sortParts = [];
        if (sortModel) {

            const groupColIds =
                rowGroupCols.map(groupCol => groupCol.id)
                    .slice(0, groupKeys.length + 1);

            sortModel.forEach(function (item) {
                if (grouping && groupColIds.indexOf(item.colId) < 0) {
                    // ignore
                } else {
                    sortParts.push(item.colId + ' ' + item.sort);
                }
            });
        }

        if (sortParts.length > 0) {
            return ' order by ' + sortParts.join(', ');
        } else {
            return '';
        }
    }

    isDoingGrouping(rowGroupCols, groupKeys) {
        // we are not doing grouping if at the lowest level. we are at the lowest level
        // if we are grouping by more columns than we have keys for (that means the user
        // has not expanded a lowest level group, OR we are not grouping at all).
        return rowGroupCols.length > groupKeys.length;
    }

    createLimitSql(request) {
        const startRow = request.startRow;
        const endRow = request.endRow;
        const pageSize = endRow - startRow;
        return ' limit ' + (pageSize + 1) + ' offset ' + startRow;
        //return ' limit 300';
    }

    getRowCount(request, results) {
        if (results === null || results === undefined || results.length === 0) {
            return null;
        }
        const currentLastRow = request.startRow + results.length;
        return currentLastRow <= request.endRow ? currentLastRow : -1;
    }

    cutResultsToPageSize(request, results) {
        const pageSize = request.endRow - request.startRow;
        if (results && results.length > pageSize) {
            return results.splice(0, pageSize);
        } else {
            return results;
        }
    }
}

module.exports = new revenueService();