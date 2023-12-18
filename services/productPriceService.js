
var pricelogger = require("../priceLogger");
class productPriceService {

    getData(connection, request, resultsCallback) {
        const SQL = this.buildSql(request);
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

    getAllProducts(connection, request, resultsCallback) {
        connection.query(request, (error, results) => {
            resultsCallback(results);
        });
    }

    getAllDebtors(connection, request, resultsCallback) {
        const SQL = "SELECT * FROM price_management_customer_groups ORDER BY sort_order ASC";
        connection.query(SQL, (error, results) => {
            resultsCallback(results);
        });
    }

    getAllProductBySkus(connection, request, resultsCallback) {
        const SQL = "SELECT * FROM price_management_data";
        connection.query(SQL, (error, results) => {
            resultsCallback(results);
        });
    }
    activateUpdatedProducts(connection, request, resultsCallback) {
        const SQL = "UPDATE price_management_data SET is_activated = '1' WHERE is_updated = '1'";
        connection.query(SQL, (error, results) => {
            resultsCallback("done");
        });
    }

    savePriceData(connection, request, resultsCallback) {

        const chunk_size = 1000;
        pricelogger.info("Total Updates:-" + request.length + " Chunk Size:-" + chunk_size);
        const chunks = Array.from({ length: Math.ceil(request.length / chunk_size) }).map(() => request.splice(0, chunk_size));

        //const chunkLength = chunks;
        //console.log(chunkLength.length);


        for (const chunk_key in chunks) {
            const value = chunks[chunk_key];

            var update_bulk_sql = "UPDATE price_management_data SET ";
            var col_sp = "selling_price = (CASE ";
            var col_pp = "profit_percentage = (CASE ";
            var col_ppsp = "profit_percentage_selling_price = (CASE ";
            var col_dgp = "discount_on_gross_price = (CASE ";
            var col_pi = "percentage_increase = (CASE ";
            var col_iu = "is_updated = (CASE ";
            var col_siu = "is_updated_skwirrel = (CASE "

            var col_debid = "";
            var col_debsp = "";
            var col_debpp = "";
            var col_debppsp = "";
            var col_debdgp = "";
            var col_debiu = "";
            var col_debsiu = "";

            var col_final_debid = "";
            var col_final_debsp = "";
            var col_final_debpp = "";
            var col_final_debppsp = "";
            var col_final_debdgp = "";
            var col_final_debiu = "";
            var col_final_debsiu = "";

            var debtor_number = "no";

            var updateArray = [];
            var historyString = "";

            for (const chunk_data in value) {
                const actual_data = value[chunk_data];

                if (typeof actual_data["debtor"] != "undefined") {
                    debtor_number = actual_data["debtor"];
                    col_debid += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data["debtor_id"] + "'";
                    col_debsp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data["group_" + debtor_number + "_debter_selling_price"] + "'";
                    col_debpp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data["group_" + debtor_number + "_margin_on_buying_price"] + "'";
                    col_debppsp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data["group_" + debtor_number + "_margin_on_selling_price"] + "'";
                    col_debdgp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data["group_" + debtor_number + "_discount_on_grossprice_b_on_deb_selling_price"] + "'";
                    col_debiu += "WHEN product_id = '" + actual_data.product_id + "' THEN '1'";
                    col_debsiu += "WHEN product_id = '" + actual_data.product_id + "' THEN '1'";
                } else {
                    col_sp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data.selling_price + "'";
                    col_pp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data.profit_percentage + "'";
                    col_ppsp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data.profit_percentage_selling_price + "'";
                    col_dgp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data.discount_on_gross_price + "'";
                    col_pi += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data.percentage_increase + "'";
                    col_iu += "WHEN product_id = '" + actual_data.product_id + "' THEN '1'";
                    col_siu += "WHEN product_id = '" + actual_data.product_id + "' THEN '1'";

                    historyString += "('" + actual_data.product_id + "','" + actual_data.webshop_net_unit_price + "','" + actual_data.webshop_gross_unit_price + "','" + actual_data.webshop_idealeverpakking + "','" + actual_data.webshop_afwijkenidealeverpakking + "','" + actual_data.webshop_buying_price + "','" + actual_data.webshop_selling_price + "','" + actual_data.buying_price + "','" + actual_data.gross_unit_price + "','" + actual_data.idealeverpakking + "','" + actual_data.afwijkenidealeverpakking + "','" + actual_data.buying_price + "','" + actual_data.selling_price + "',now(),'Price Management','No','" + JSON.stringify(Array('new_selling_price')) + "','0','No'),";
                }
                updateArray.push(actual_data.product_id);
            }

            historyString = historyString.replace(/,+$/, '');

            col_sp += " END)";
            col_pp += " END)";
            col_ppsp += " END)";
            col_dgp += " END)";
            col_pi += " END)";
            col_iu += " END)";
            col_siu += " END)";
            //create_case_statement_for_price += " END";
            var get_all_products_to_update = updateArray.join(",");

            if (debtor_number != "no") { //If debtors
                var col_final_debid = "group_" + debtor_number + "_magento_id = (CASE " + col_debid + " END)";
                var col_final_debsp = "group_" + debtor_number + "_debter_selling_price = (CASE " + col_debsp + " END)";
                var col_final_debpp = "group_" + debtor_number + "_margin_on_buying_price = (CASE " + col_debpp + " END)";
                var col_final_debppsp = "group_" + debtor_number + "_margin_on_selling_price = (CASE " + col_debppsp + " END)";
                var col_final_debdgp = "group_" + debtor_number + "_discount_on_grossprice_b_on_deb_selling_price = (CASE " + col_debdgp + " END)";
                var col_final_debiu = "is_updated = (CASE " + col_debiu + " END)";
                var col_final_debsiu = "is_updated_skwirrel = (CASE " + col_debsiu + " END)";
                update_bulk_sql += col_final_debid + ', ' + col_final_debsp + ', ' + col_final_debpp + ', ' + col_final_debppsp + ', ' + col_final_debdgp + ', ' + col_final_debiu + ',' + col_final_debsiu + ' WHERE product_id IN (' + get_all_products_to_update + ')';
            } else {
                update_bulk_sql += col_sp + ', ' + col_pp + ', ' + col_ppsp + ', ' + col_dgp + ', ' + col_pi + ', ' + col_iu + ', ' + col_siu + ' WHERE product_id IN (' + get_all_products_to_update + ')';
            }

            pricelogger.info("Processing Chunk " + chunk_key + ":-" + update_bulk_sql);
            connection.query(update_bulk_sql, (error, results) => {
                if (error) {
                    pricelogger.error(error.message);
                }
                if (debtor_number == "no") {
                    connection.query("INSERT INTO price_management_history (product_id,old_net_unit_price,old_gross_unit_price,old_idealeverpakking,old_afwijkenidealeverpakking,old_buying_price,old_selling_price,new_net_unit_price,new_gross_unit_price,new_idealeverpakking,new_afwijkenidealeverpakking,new_buying_price,new_selling_price,updated_date_time,updated_by,is_viewed,fields_changed,buying_price_changed,is_synced) VALUES " + historyString + "");
                }
            });

        }
        resultsCallback("done");
    }

    uploadPriceData(connection, request, resultsCallback) {
        const chunk_size = 15000;
        const chunks = Array.from({ length: Math.ceil(request[1].length / chunk_size) }).map(() => request[1].splice(0, chunk_size));
        var allCols = request[0].split(",");
        for (const chunk_key in chunks) {
            const value = chunks[chunk_key];
            var uploadData = "";
            var chunkStatus = Array();
            var sql = "INSERT INTO price_management_data (" + request[0] + ") VALUES ";
            for (const chunk_data in value) {
                uploadData += "(";
                allCols.forEach((col) => {
                    uploadData += '"' + value[chunk_data][col] + '"' + ",";
                });
                uploadData = uploadData.replace(/,+$/, '');
                uploadData += "),";

            }
            uploadData = uploadData.replace(/,+$/, '');
            sql += "" + uploadData + " ON DUPLICATE KEY UPDATE " + request[2] + "";
            //pricelogger.info("Upload Chunk " + chunk_key + ":- " + sql);
            connection.query(sql, (error, results) => {
                if (error) {
                    pricelogger.error("Upload Chunk " + chunk_key + " ERROR :- " + error.message);
                }
            });


        }

        // For History
        if (request[3].length >= 1) {
            const history_chunk_size = 15000;
            const history_chunks = Array.from({ length: Math.ceil(request[3].length / history_chunk_size) }).map(() => request[3].splice(0, history_chunk_size));
            var historyCols = "product_id,old_net_unit_price,old_gross_unit_price,old_idealeverpakking,old_afwijkenidealeverpakking,old_buying_price,old_selling_price,new_net_unit_price,new_gross_unit_price,new_idealeverpakking,new_afwijkenidealeverpakking,new_buying_price,new_selling_price,updated_date_time,updated_by,is_viewed,fields_changed,buying_price_changed";
            var allHistoryCols = historyCols.split(",");

            for (const history_chunk_key in history_chunks) {
                const history_value = history_chunks[history_chunk_key];
                var uploadHistoryData = "";
                var historySql = "INSERT INTO price_management_history (" + historyCols + ") VALUES ";

                for (const history_chunk_data in history_value) {
                    uploadHistoryData += "(";
                    allHistoryCols.forEach((hisCol) => {
                        uploadHistoryData += "'" + history_value[history_chunk_data][hisCol] + "'" + ",";
                    });
                    uploadHistoryData = uploadHistoryData.replace(/,+$/, '');
                    uploadHistoryData += "),";
                }

                uploadHistoryData = uploadHistoryData.replace(/,+$/, '');
                historySql += "" + uploadHistoryData + "";
                // pricelogger.info("Upload History Chunk " + history_chunk_key + ":- " + historySql);
                connection.query(historySql, (error, results) => {
                    if (error) {
                        pricelogger.error("Upload History Chunk " + history_chunk_key + " ERROR :- " + error.message);
                    }
                });

            }
        }

        resultsCallback("done");


    }

    insertChunk(connection, chunk_key, sql) {
        return new Promise((resolve, reject) => {
            connection.query(sql, (error, results) => {
                if (error) {
                    //reject("Error in chunk " + chunk_key + ": " + error.message + "");
                    reject("Error in chunk " + chunk_key + "");
                } else {
                    resolve("Success Chunk " + chunk_key + "");
                }
            });
        });
    }

    buildSql(request, totalrecords = "") {

        const selectSql = this.createSelectSql(request);
        var category_join = "";
        if ((request.cats).length > 0) {
            category_join = "INNER JOIN price_management_catpro AS pmcp ON pmcp.product_id = pmd.product_id "
        }
        const fromSql = " FROM price_management_data AS pmd " + category_join + "";
        const whereSql = this.createWhereSql(request);
        const limitSql = this.createLimitSql(request);

        const orderBySql = this.createOrderBySql(request);
        const groupBySql = this.createGroupBySql(request);

        if (totalrecords == "show") {
            var SQLCOUNT = this.buildsqlcount(fromSql, whereSql);
            return SQLCOUNT;
        } else {
            const SQL = selectSql + fromSql + whereSql + groupBySql + orderBySql + limitSql;
            // console.log(SQL);
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

        const all_d_cols = [];
        for (let d = 0; d <= 15; d++) {
            var cust_group = parseInt(4027100 + d);
            all_d_cols.push("pmd.group_" + cust_group + "_debter_selling_price",
                "pmd.group_" + cust_group + "_margin_on_buying_price",
                "pmd.group_" + cust_group + "_margin_on_selling_price",
                "pmd.group_" + cust_group + "_discount_on_grossprice_b_on_deb_selling_price");
        }

        return ' select DISTINCT pmd.product_id, pmd.supplier_type, pmd.name, pmd.sku, pmd.supplier_sku, pmd.eancode, pmd.merk, pmd.idealeverpakking, pmd.afwijkenidealeverpakking, pmd.categories, pmd.buying_price, pmd.selling_price, pmd.profit_percentage, pmd.profit_percentage_selling_price, pmd.discount_on_gross_price, pmd.percentage_increase, pmd.magento_status, pmd.gross_unit_price, CAST((1 - (pmd.net_unit_price / CASE WHEN (pmd.gross_unit_price = 0) THEN 1 ELSE (pmd.gross_unit_price) END )) * 100 AS DECIMAL (10 , 4 )) AS supplier_discount_gross_price, pmd.webshop_selling_price, pmd.net_unit_price, pmd.is_updated, pmd.is_updated_skwirrel, pmd.is_activated, pmd.webshop_net_unit_price, pmd.webshop_gross_unit_price, pmd.webshop_idealeverpakking, pmd.webshop_afwijkenidealeverpakking, pmd.webshop_buying_price, (SELECT COUNT(*) AS mag_updated_product_cnt FROM price_management_history WHERE product_id = pmd.product_id and is_viewed = "No" and updated_by = "Magento" and buying_price_changed = "1") AS mag_updated_product_cnt, ' + all_d_cols.toString() + '';
    }

    createFilterSql(key, item) {
        //console.log(key + "===" + item + "===" + item.filterType);

        switch (item.filterType) {
            case 'text':

                let i = 1;
                let condition_making_new = '';
                let condition_making = '';
                for (i = 1; i <= 2; i++) {
                    let condition = 'condition' + i;
                    condition_making = this.createTextFilterSql(key, item[condition]);
                    if (condition_making_new != '') {
                        condition_making_new = condition_making_new + ' ' + item.operator + ' ' + condition_making;
                    } else {
                        condition_making_new = condition_making;
                    }
                }
                return condition_making_new;
            case 'number':
                let j = 1;
                let condition_making_new_j = '';
                let condition_making_j = '';
                const result = this.countKeysMatchingString(item, 'condition');

                if (result == 0) {
                    return this.createNumberFilterSql(key, item);
                } else {
                    for (j = 1; j <= 2; j++) {
                        let condition = 'condition' + j;

                        condition_making_j = this.createNumberFilterSql(key, item[condition]);
                        if (condition_making_new_j != '') {
                            condition_making_new_j = condition_making_new_j + ' ' + item.operator + ' ' + condition_making_j;
                        } else {
                            condition_making_new_j = condition_making_j;
                        }
                    }
                    return condition_making_new_j;
                }

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
        //console.log(key + "===" + item.type);
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
            //console.log(keySet);
            keySet.forEach(function (key) {
                const item = filterModel[key];
                whereParts.push(that.createFilterSql(key, item));
            });
        }

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

        /*
        var cat_filter = "";
        if ((request.cats).length > 0) {
            cat_filter = 'pmcp.category_id IN (' + request.cats + ')';
        }
    
        if (whereParts.length > 0) {
            if (cat_filter == "") {
                return ' where ' + whereParts.join(' and ') + '';
            } else {
                return ' where ' + whereParts.join(' and ') + ' and ' + cat_filter + '';
            }
        } else {
            if (cat_filter == "") {
                return '';
            } else {
                return 'where pmcp.category_id IN (' + request.cats + ')';
            }
        }
        */

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


    getCategoryBrand(connection, request, resultsCallback) {
        let selected_cats = request['selected_cats'];
        let cat_que = "";

        if (selected_cats != "") {
            cat_que = " WHERE catpro.category_id IN (" + selected_cats + ")";
        }
        let SQL = "SELECT pmd.id, pmd.merk AS product_count, pmd.supplier_type FROM price_management_catpro AS catpro INNER JOIN price_management_data AS pmd ON pmd.product_id = catpro.product_id " + cat_que + " group by pmd.merk ORDER BY pmd.merk ASC";
        connection.query(SQL, (error, results) => {
            resultsCallback(results);
        });
    }

    savePriceData(connection, request, resultsCallback) {

        const chunk_size = 1000;

        pricelogger.info("Total Updates:-" + request.length + " Chunk Size:-" + chunk_size);
        const chunks = Array.from({ length: Math.ceil(request.length / chunk_size) }).map(() => request.splice(0, chunk_size));

        //const chunkLength = chunks;
        //console.log(chunkLength.length);


        for (const chunk_key in chunks) {
            const value = chunks[chunk_key];

            var update_bulk_sql = "UPDATE price_management_data SET ";
            var col_sp = "selling_price = (CASE ";
            var col_pp = "profit_percentage = (CASE ";
            var col_ppsp = "profit_percentage_selling_price = (CASE ";
            var col_dgp = "discount_on_gross_price = (CASE ";
            var col_pi = "percentage_increase = (CASE ";
            var col_iu = "is_updated = (CASE ";
            var col_siu = "is_updated_skwirrel = (CASE "

            var col_debid = "";
            var col_debsp = "";
            var col_debpp = "";
            var col_debppsp = "";
            var col_debdgp = "";
            var col_debiu = "";
            var col_debsiu = "";

            var col_final_debid = "";
            var col_final_debsp = "";
            var col_final_debpp = "";
            var col_final_debppsp = "";
            var col_final_debdgp = "";
            var col_final_debiu = "";
            var col_final_debsiu = "";

            var debtor_number = "no";

            var updateArray = [];
            var historyString = "";

            for (const chunk_data in value) {
                const actual_data = value[chunk_data];

                if (typeof actual_data["debtor"] != "undefined") {

                    //if that product_id does not exist then continue

                    debtor_number = actual_data["debtor"];


                    col_debid += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data["debtor_id"] + "'";
                    col_debsp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data["group_" + debtor_number + "_debter_selling_price"] + "'";
                    col_debpp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data["group_" + debtor_number + "_margin_on_buying_price"] + "'";
                    col_debppsp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data["group_" + debtor_number + "_margin_on_selling_price"] + "'";
                    col_debdgp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data["group_" + debtor_number + "_discount_on_grossprice_b_on_deb_selling_price"] + "'";
                    col_debiu += "WHEN product_id = '" + actual_data.product_id + "' THEN '1'";
                    col_debsiu += "WHEN product_id = '" + actual_data.product_id + "' THEN '1'";
                } else {
                    col_sp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data.selling_price + "'";
                    col_pp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data.profit_percentage + "'";
                    col_ppsp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data.profit_percentage_selling_price + "'";
                    col_dgp += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data.discount_on_gross_price + "'";
                    col_pi += "WHEN product_id = '" + actual_data.product_id + "' THEN '" + actual_data.percentage_increase + "'";
                    col_iu += "WHEN product_id = '" + actual_data.product_id + "' THEN '1'";
                    col_siu += "WHEN product_id = '" + actual_data.product_id + "' THEN '1'";

                    historyString += "('" + actual_data.product_id + "','" + actual_data.webshop_net_unit_price + "','" + actual_data.webshop_gross_unit_price + "','" + actual_data.webshop_idealeverpakking + "','" + actual_data.webshop_afwijkenidealeverpakking + "','" + actual_data.webshop_buying_price + "','" + actual_data.webshop_selling_price + "','" + actual_data.buying_price + "','" + actual_data.gross_unit_price + "','" + actual_data.idealeverpakking + "','" + actual_data.afwijkenidealeverpakking + "','" + actual_data.buying_price + "','" + actual_data.selling_price + "',now(),'Price Management','No','" + JSON.stringify(Array('new_selling_price')) + "','0','No'),";
                }
                updateArray.push(actual_data.product_id);
            }

            historyString = historyString.replace(/,+$/, '');

            col_sp += " END)";
            col_pp += " END)";
            col_ppsp += " END)";
            col_dgp += " END)";
            col_pi += " END)";
            col_iu += " END)";
            col_siu += " END)";
            //create_case_statement_for_price += " END";
            var get_all_products_to_update = updateArray.join(",");

            if (debtor_number != "no") { //If debtors
                var col_final_debid = "group_" + debtor_number + "_magento_id = (CASE " + col_debid + " END)";
                var col_final_debsp = "group_" + debtor_number + "_debter_selling_price = (CASE " + col_debsp + " END)";
                var col_final_debpp = "group_" + debtor_number + "_margin_on_buying_price = (CASE " + col_debpp + " END)";
                var col_final_debppsp = "group_" + debtor_number + "_margin_on_selling_price = (CASE " + col_debppsp + " END)";
                var col_final_debdgp = "group_" + debtor_number + "_discount_on_grossprice_b_on_deb_selling_price = (CASE " + col_debdgp + " END)";
                var col_final_debiu = "is_updated = (CASE " + col_debiu + " END)";
                var col_final_debsiu = "is_updated_skwirrel = (CASE " + col_debsiu + " END)";
                update_bulk_sql += col_final_debid + ', ' + col_final_debsp + ', ' + col_final_debpp + ', ' + col_final_debppsp + ', ' + col_final_debdgp + ', ' + col_final_debiu + ',' + col_final_debsiu + ' WHERE product_id IN (' + get_all_products_to_update + ')';
            } else {
                update_bulk_sql += col_sp + ', ' + col_pp + ', ' + col_ppsp + ', ' + col_dgp + ', ' + col_pi + ', ' + col_iu + ', ' + col_siu + ' WHERE product_id IN (' + get_all_products_to_update + ')';
            }

            pricelogger.info("Processing Chunk " + chunk_key + ":-" + update_bulk_sql);
            connection.query(update_bulk_sql, (error, results) => {
                if (error) {
                    pricelogger.error(error.message);
                }
                if (debtor_number == "no") {
                    connection.query("INSERT INTO price_management_history (product_id,old_net_unit_price,old_gross_unit_price,old_idealeverpakking,old_afwijkenidealeverpakking,old_buying_price,old_selling_price,new_net_unit_price,new_gross_unit_price,new_idealeverpakking,new_afwijkenidealeverpakking,new_buying_price,new_selling_price,updated_date_time,updated_by,is_viewed,fields_changed,buying_price_changed,is_synced) VALUES " + historyString + "");
                }
            });

        }
        resultsCallback("done");
    }

    countKeysMatchingString(obj, searchString) {
        let count = 0;

        // Iterate through the keys of the object
        for (const key in obj) {
            // Check if the key includes the specified string
            if (key.includes(searchString)) {
                count++;
            }
        }

        return count;
    }
}

module.exports = new productPriceService();