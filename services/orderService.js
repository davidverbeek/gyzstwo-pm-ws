class orderService {

    getData(connection, request, resultsCallback) {
        var SQL = this.buildSql(request);
        connection.query(SQL, (error, results) => {
            const rowCount = this.getRowCount(request, results);
            const resultsForPage = this.cutResultsToPageSize(request, results);
            const currentSql = SQL;
            resultsCallback(resultsForPage, rowCount, currentSql);
        });
    }

    getDataCount(connection, request, resultsCallback) {
        var SQL = this.buildSql(request, "show");
        connection.query(SQL, (err, rows, fields) => {
            resultsCallback(results[0]["TOTAL_RECORDS"]);
        });
    }

    buildSql(request, totalrecords = "") {

        const selectSql = this.createSelectSql(request);
        const fromSql = " FROM sales_order AS so INNER JOIN sales_order_item AS soi ON so.entity_id = soi.order_id AND so.created_at >= '" + request.revenueStartDate + "' AND so.created_at <= '" + request.revenueEndDate + "' AND soi.sku = '" + request.revenueSku + "'";
        const whereSql = this.createWhereSql(request);

        var M1SELECTFROMWHERE = this.buildSqlM1(request);
        var M1SQL = M1SELECTFROMWHERE[0] + M1SELECTFROMWHERE[1] + M1SELECTFROMWHERE[2]
        const limitSql = this.createLimitSql(request);
        const orderBySql = this.createOrderBySql(request);
        const groupBySql = this.createGroupBySql(request);

        if (totalrecords == "show") {
            var SQLCOUNT = this.buildsqlcount(fromSql, whereSql, M1SELECTFROMWHERE[1], M1SELECTFROMWHERE[2]);
            return SQLCOUNT;
        } else {
            const SQL = selectSql + fromSql + whereSql + M1SQL + groupBySql + orderBySql + limitSql;
            return SQL;
        }
    }


    buildSqlM1(request) {

        var selectFromWhere = [];

        const selectSqlm1 = "UNION SELECT mso.state, mso.created_at AS created_at,CONCAT(msoi.order_id, '_m1') AS order_id,msoi.qty_ordered AS qty_ordered,msoi.qty_refunded AS qty_refunded,msoi.base_cost AS base_cost,msoi.base_price AS base_price,(CASE msoi.afwijkenidealeverpakking WHEN 0 THEN (msoi.base_cost * msoi.qty_ordered * msoi.idealeverpakking)  ELSE (msoi.base_cost * msoi.qty_ordered) END) AS cost,(msoi.base_price * msoi.qty_ordered) AS price,((msoi.base_price * msoi.qty_ordered) - (CASE msoi.afwijkenidealeverpakking WHEN 0 THEN (msoi.base_cost * msoi.qty_ordered * msoi.idealeverpakking)  ELSE (msoi.base_cost * msoi.qty_ordered) END) ) AS absolute_margin,msoi.afwijkenidealeverpakking AS afwijkenidealeverpakking,msoi.idealeverpakking AS idealeverpakking";
        const fromSqlm1 = " FROM mage_sales_flat_order AS mso INNER JOIN mage_sales_flat_order_item AS msoi ON mso.entity_id = msoi.order_id AND mso.created_at >= '" + request.revenueStartDate + "' AND mso.created_at <= '" + request.revenueEndDate + "' AND msoi.m2_sku = '" + request.revenueSku + "'";
        const whereSqlm1 = this.createWhereSqlM1(request);
        selectFromWhere[0] = selectSqlm1;
        selectFromWhere[1] = fromSqlm1;
        selectFromWhere[2] = whereSqlm1;
        return selectFromWhere;
    }

    buildsqlcount(fromSql, whereSql, fromSqlM1, whereSqlM1) {
        var countsql = "";
        //return countsql = "SELECT COUNT(*) AS TOTAL_RECORDS " + fromSql + " " + whereSql + "";
        return countsql = "SELECT COUNT(*) AS TOTAL_RECORDS FROM (SELECT so.entity_id " + fromSql + " " + whereSql + " UNION (SELECT mso.entity_id " + fromSqlM1 + " " + whereSqlM1 + ")) m2m1";
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

        return ' SELECT so.state, so.created_at AS created_at,soi.order_id AS order_id,soi.qty_ordered AS qty_ordered,soi.qty_refunded AS qty_refunded,soi.base_cost AS base_cost,soi.base_price AS base_price,(CASE soi.afwijkenidealeverpakking WHEN 0 THEN (soi.base_cost * soi.qty_ordered * soi.idealeverpakking)  ELSE (soi.base_cost * soi.qty_ordered) END) AS cost,(soi.base_price * soi.qty_ordered) AS price,((soi.base_price * soi.qty_ordered) - (CASE soi.afwijkenidealeverpakking WHEN 0 THEN (soi.base_cost * soi.qty_ordered * soi.idealeverpakking)  ELSE (soi.base_cost * soi.qty_ordered) END) ) AS absolute_margin,soi.afwijkenidealeverpakking AS afwijkenidealeverpakking,soi.idealeverpakking AS idealeverpakking';
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
        console.log(key + "===" + item.type);
        switch (item.type) {
            case 'equals':
                return key + ' = ' + item.filter;
            case 'notEqual':
                return key + ' != ' + item.filter;
            case 'greaterThan':
                return key + ' > ' + item.filter;
            case 'greaterThanOrEqual':
                return key + ' >= ' + item.filter;
            case 'lessThan':
                return key + ' < ' + item.filter;
            case 'lessThanOrEqual':
                return key + ' <= ' + item.filter;
            case 'inRange':
                return '(' + key + ' >= ' + item.filter + ' and ' + key + ' <= ' + item.filterTo + ')';
            default:
                console.log('unknown number filter type: ' + item.type);
                return 'true';
        }
    }

    createTextFilterSql(key, item) {
        switch (item.type) {
            case 'equals':
                return key + ' = "' + item.filter + '"';
            case 'notEqual':
                return key + ' != "' + item.filter + '"';
            case 'contains':
                return key + ' like "%' + item.filter + '%"';
            case 'notContains':
                return key + ' not like "%' + item.filter + '%"';
            case 'startsWith':
                return key + ' like "' + item.filter + '%"';
            case 'endsWith':
                return key + ' like "%' + item.filter + '"';
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

        var defaultWhereCondition = "(so.state != 'canceled' AND so.state != 'new' AND so.state != 'pending_payment')";

        if (whereParts.length > 0) {
            return ' where ' + defaultWhereCondition + ' and ' + whereParts.join(' and ');
        } else {
            return ' where ' + defaultWhereCondition + '';
        }
    }

    createWhereSqlM1(request) {
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

        var defaultWhereCondition = "(mso.state != 'canceled' AND mso.state != 'new' AND mso.state != 'pending_payment')";

        if (whereParts.length > 0) {
            return ' where ' + defaultWhereCondition + ' and ' + whereParts.join(' and ');
        } else {
            return ' where ' + defaultWhereCondition + '';
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

module.exports = new orderService();