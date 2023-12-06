class bolMinimumService {

    getData(connection, request, resultsCallback) {
        const SQL = this.buildSql(request);
        //console.log(SQL);
        connection.query(SQL, (error, results) => {
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

        const fromSql = " FROM price_management_bol_minimum AS pmbm LEFT JOIN price_management_ec_deliverytime AS pmecdtime ON pmecdtime.option_id = pmbm.ec_deliverytime LEFT JOIN price_management_ec_deliverytime_be AS pmecdtimebe ON pmecdtimebe.option_id = pmbm.ec_deliverytime_be";
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
        return countsql = "SELECT COUNT(*) AS TOTAL_RECORDS " + fromSql + " " + whereSql + "";
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

        return ' select pmbm.*, pmecdtime.option_value AS ec_deliverytime_text, pmecdtimebe.option_value AS ec_deliverytime_be_text';
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

        if (whereParts.length > 0) {
            return ' where ' + whereParts.join(' and ');
        } else {
            return '';
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

    getAllECDeliveryTimes(connection, request, resultsCallback) {
        const SQL = "SELECT * FROM price_management_ec_deliverytime";
        connection.query(SQL, (error, results) => {
            resultsCallback(results);
        });
    }
    getAllECDeliveryTimesBE(connection, request, resultsCallback) {
        const SQL = "SELECT * FROM price_management_ec_deliverytime_be";
        connection.query(SQL, (error, results) => {
            resultsCallback(results);
        });
    }

    saveBolDeliveryTime(connection, request, resultsCallback) {
        const chunk_size = 3;
        const chunks = Array.from({ length: Math.ceil(request.length / chunk_size) }).map(() => request.splice(0, chunk_size));
        var allCols = ["product_id", "ec_deliverytime", "updated_date_time"];
        //var totalUpdated = Array();
        for (const chunk_key in chunks) {
            const value = chunks[chunk_key];
            var uploadData = "";
            var chunkStatus = Array();
            var sql = "INSERT INTO price_management_bol_minimum (product_id,ec_deliverytime,updated_date_time) VALUES ";
            for (const chunk_data in value) {
                uploadData += "(";
                allCols.forEach((col) => {
                    uploadData += '"' + value[chunk_data][col] + '"' + ",";
                });
                uploadData = uploadData.replace(/,+$/, '');
                uploadData += "),";
            }
            uploadData = uploadData.replace(/,+$/, '');
            sql += "" + uploadData + " ON DUPLICATE KEY UPDATE ec_deliverytime = VALUES(ec_deliverytime), updated_date_time = now()";
            //totalUpdated.push(value.length);
            connection.query(sql, (error, results) => {
                if (error) {
                    return console.error(error.message);
                }

            });
        }

        // Using reduce function to find the sum
        /* let sum = totalUpdated.reduce(function (x, y) {
            return x + y;
        }, 0); */

        resultsCallback("done");
    }

    saveBolDeliveryTimeBE(connection, request, resultsCallback) {
        const chunk_size = 3;
        const chunks = Array.from({ length: Math.ceil(request.length / chunk_size) }).map(() => request.splice(0, chunk_size));
        var allCols = ["product_id", "ec_deliverytime_be", "updated_date_time"];
        //var totalUpdated = Array();
        for (const chunk_key in chunks) {
            const value = chunks[chunk_key];
            var uploadData = "";
            var chunkStatus = Array();
            var sql = "INSERT INTO price_management_bol_minimum (product_id,ec_deliverytime_be,updated_date_time) VALUES ";
            for (const chunk_data in value) {
                uploadData += "(";
                allCols.forEach((col) => {
                    uploadData += '"' + value[chunk_data][col] + '"' + ",";
                });
                uploadData = uploadData.replace(/,+$/, '');
                uploadData += "),";
            }
            uploadData = uploadData.replace(/,+$/, '');
            sql += "" + uploadData + " ON DUPLICATE KEY UPDATE ec_deliverytime_be = VALUES(ec_deliverytime_be), updated_date_time = now()";
            connection.query(sql, (error, results) => {
                if (error) {
                    return console.error(error.message);
                }

            });
        }

        resultsCallback("done");
    }



}

module.exports = new bolMinimumService();