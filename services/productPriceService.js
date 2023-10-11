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


        return ' select distinct pmd.product_id, pmd.supplier_type, pmd.name, pmd.sku, pmd.supplier_sku, pmd.eancode, pmd.merk, pmd.idealeverpakking, pmd.afwijkenidealeverpakking, pmd.buying_price, pmd.selling_price, pmd.profit_percentage, pmd.profit_percentage_selling_price, pmd.discount_on_gross_price, pmd.percentage_increase, pmd.magento_status, pmd.gross_unit_price, CAST((1 - (pmd.net_unit_price / CASE WHEN (pmd.gross_unit_price = 0) THEN 1 ELSE (pmd.gross_unit_price) END )) * 100 AS DECIMAL (10 , 4 )) AS supplier_discount_gross_price, pmd.webshop_selling_price, pmd.net_unit_price, pmd.is_updated, pmd.webshop_net_unit_price, pmd.webshop_gross_unit_price, pmd.webshop_idealeverpakking, pmd.webshop_afwijkenidealeverpakking, pmd.webshop_buying_price, ' + all_d_cols.toString() + '';
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
        let SQL = "SELECT pmd.id, pmd.merk AS product_count, pmd.supplier_type FROM price_management_catpro AS catpro INNER JOIN price_management_data AS pmd ON pmd.product_id = catpro.product_id AND " + cat_que + " group by pmd.merk ORDER BY pmd.merk ASC";

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
}

module.exports = new productPriceService();