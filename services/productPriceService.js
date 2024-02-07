
var pricelogger = require("../priceLogger");


class productPriceService {

    percentage_uploaded = 0;

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
    /*
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
    */

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async uploadPriceData(connection, request, fs, fpath, resultsCallback) {

        const batchSize = 1000; // Number of iterations to process in each batch
        let buffer = [];
        const chunk_size = 1000;
        var total_products = request[1].length;
        const chunks = Array.from({ length: Math.ceil(request[1].length / chunk_size) }).map(() => request[1].splice(0, chunk_size));
        var allCols = request[0].split(",");
        var current_product = 1;
        var p = {};
        for (const chunk_key in chunks) {
            const value = chunks[chunk_key];
            var uploadData = "";
            var chunkStatus = Array();
            var sql = "INSERT INTO price_management_data (" + request[0] + ") VALUES ";
            var chunk_current_product = 1;
            for (const chunk_data in value) {

                buffer.push(chunk_current_product);

                uploadData += "(";
                allCols.forEach((col) => {
                    uploadData += '"' + value[chunk_data][col] + '"' + ",";
                });

                uploadData = uploadData.replace(/,+$/, '');
                uploadData += "),";
                this.percentage_uploaded = parseFloat(current_product / total_products) * 100;
                if (buffer.length === batchSize) {
                    //io.emit("showUploadProgress", parseInt(this.percentage_uploaded));
                    p["cnt"] = parseInt(this.percentage_uploaded);
                    fs.writeFile(fpath + '/progress.txt', JSON.stringify(p), (err) => {
                        if (err) throw err;
                    });

                    await this.sleep(1000);
                    buffer = [];
                }
                chunk_current_product++;
                current_product++;
            }

            uploadData = uploadData.replace(/,+$/, '');
            sql += "" + uploadData + " ON DUPLICATE KEY UPDATE " + request[2] + "";
            connection.query(sql, (error, results) => {
                if (error) {
                    pricelogger.error("Upload Chunk " + chunk_key + " ERROR :- " + error.message);
                }
            });
        }

        // Emit any remaining items in the buffer
        if (buffer.length > 0) {
            //io.emit('showUploadProgress', parseInt(this.percentage_uploaded));
            p["cnt"] = parseInt(this.percentage_uploaded);
            fs.writeFile(fpath + '/progress.txt', JSON.stringify(p), (err) => {
                if (err) throw err;
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

        resultsCallback(total_products);


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

        const fromSql = " FROM price_management_data AS pmd " + category_join + "LEFT JOIN bigshopper_prices AS mktpr ON mktpr.product_id = pmd.product_id" + " " + "LEFT JOIN revenue_study_price_management AS rspm ON rspm.product_id = pmd.product_id" + " ";
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
        return countsql = "SELECT COUNT(DISTINCT pmd.product_id) AS TOTAL_RECORDS " + fromSql + " " + whereSql + "";
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
        const bs_cols = "CASE WHEN mktpr.lowest_price IS NOT NULL THEN mktpr.lowest_price  ELSE '---' END AS lowest_price " + ',' + "CASE WHEN mktpr.highest_price IS NOT NULL THEN mktpr.highest_price ELSE '---' END AS highest_price" + ',' + "CASE WHEN mktpr.lp_diff_percentage IS NOT NULL THEN mktpr.lp_diff_percentage ELSE '---' END AS lp_diff_percentage" + ',' + "CASE WHEN mktpr.hp_diff_percentage IS NOT NULL THEN mktpr.hp_diff_percentage ELSE '---' END AS hp_diff_percentage" + ',' + "CASE WHEN mktpr.price_competition_score IS NOT NULL THEN mktpr.price_competition_score ELSE '---' END AS price_competition_score" + ',' + "CASE WHEN mktpr.position IS NOT NULL THEN mktpr.position ELSE '---' END AS position" + ',' + "CASE WHEN mktpr.number_competitors IS NOT NULL THEN mktpr.number_competitors ELSE '---' END AS number_competitors" + ',' + "CASE WHEN mktpr.productset_incl_dispatch IS NOT NULL THEN mktpr.productset_incl_dispatch ELSE '---' END AS productset_incl_dispatch" + "," + "CASE WHEN mktpr.price_of_the_next_excl_shipping IS NOT NULL THEN mktpr.price_of_the_next_excl_shipping ELSE '---' END AS price_of_the_next_excl_shipping";
        const rev_cols = "CASE WHEN (rspm.current_revenue IS NOT NULL && rspm.previous_revenue IS NOT NULL) THEN CONCAT(rspm.current_revenue,' == ',rspm.previous_revenue) ELSE CONCAT( ifnull(current_revenue,'00'),' == ',ifnull(previous_revenue,'00')) END AS compare_revenue_60";
        const rev_cols_1 = "rspm.percentage_revenue AS percentage_revenue";
        const rev_cols_2 = "CASE WHEN (rspm.current_revenue IS NOT NULL && rspm.last_year_current_revenue IS NOT NULL) THEN CONCAT(rspm.current_revenue,' == ',rspm.last_year_current_revenue) ELSE CONCAT( ifnull(current_revenue,00),' == ',ifnull(last_year_current_revenue,00)) END AS compare_revenue_year";
        const rev_cols_3 = "rspm.last_year_percentage_revenue AS last_year_percentage_revenue";

        return ' select DISTINCT pmd.product_id, pmd.supplier_type, pmd.name, pmd.sku, pmd.supplier_sku, pmd.eancode, pmd.merk, pmd.idealeverpakking, pmd.afwijkenidealeverpakking, pmd.categories, pmd.buying_price, pmd.selling_price, pmd.profit_percentage, pmd.profit_percentage_selling_price, pmd.discount_on_gross_price, pmd.percentage_increase, pmd.magento_status, pmd.gross_unit_price, CAST((1 - (pmd.net_unit_price / CASE WHEN (pmd.gross_unit_price = 0) THEN 1 ELSE (pmd.gross_unit_price) END )) * 100 AS DECIMAL (10 , 4 )) AS supplier_discount_gross_price, pmd.webshop_selling_price, pmd.net_unit_price, pmd.is_updated, pmd.is_updated_skwirrel, pmd.is_activated, pmd.webshop_net_unit_price, pmd.webshop_gross_unit_price, pmd.webshop_idealeverpakking, pmd.webshop_afwijkenidealeverpakking, pmd.webshop_buying_price, (SELECT COUNT(*) AS mag_updated_product_cnt FROM price_management_history WHERE product_id = pmd.product_id and is_viewed = "No" and updated_by = "Magento" and buying_price_changed = "1") AS mag_updated_product_cnt, ' + all_d_cols.toString() + ',' + bs_cols + ',' + rev_cols + '' + ',' + rev_cols_1 + '' + ',' + rev_cols_2 + '' + ',' + rev_cols_3 + '';
    }

    createFilterSql(key, item) {
        //console.log(key + "===" + item + "===" + item.filterType);

        switch (item.filterType) {
            case 'text':

                let i = 1;
                let condition_making_new = '';
                let condition_making = '';
                const result_text = this.countKeysMatchingString(item, 'condition');

                if (result_text == 0) {
                    return '(' + this.createTextFilterSql(key, item) + ')';
                } else {
                    for (i = 1; i <= 2; i++) {
                        let condition = 'condition' + i;
                        condition_making = this.createTextFilterSql(key, item[condition]);
                        if (condition_making_new != '') {
                            condition_making_new = condition_making_new + ' ' + item.operator + ' ' + condition_making;
                        } else {
                            condition_making_new = condition_making;
                        }
                    }
                    return '(' + condition_making_new + ')';
                }
            case 'number':
                let j = 1;
                let condition_making_new_j = '';
                let condition_making_j = '';
                const result = this.countKeysMatchingString(item, 'condition');

                if (result == 0) {
                    return '(' + this.createNumberFilterSql(key, item) + ')';
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
                    return '(' + condition_making_new_j + ')';
                }

            case 'set':
                return this.createSetFilterSql(key, item);
            default:
                console.log('unkonwn filter type: ' + item.filterType);
        }
    }

    createSetFilterSql(key, item) {
        var ptable = ["highest_price", "lowest_price", "lp_diff_percentage", "hp_diff_percentage"];
        var col_prefix = "pmd.";
        if (ptable.includes(key)) {
            var col_prefix = "mktpr.";
        }

        var allValues = (item.values).join('","');
        return col_prefix + key + ' IN ("' + allValues + '")';
    }

    createNumberFilterSql(key, item) {
        //console.log(key + "===" + item.type);

        var ptable = ["highest_price", "lowest_price", "lp_diff_percentage", "hp_diff_percentage"];
        var rev_table = ["current_revenue", "previous_revenue", "last_year_percentage_revenue", "percentage_revenue"];
        var col_prefix = "pmd.";
        if (ptable.includes(key)) {
            col_prefix = "mktpr.";
        } else if (rev_table.includes(key)) {
            col_prefix = "rspm.";
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
        var ptable = ["highest_price", "lowest_price", "lp_diff_percentage", "hp_diff_percentage"];
        var col_prefix = "pmd.";
        if (ptable.includes(key)) {
            var col_prefix = "mktpr.";
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


    updateToBsOptions(connection, request, resultsCallback) {

        let product_to_update_arr = [];
        let sql_chk_all;
        let from;
        let response_data = [];
        if (request.isAllChecked == 1) {
            // Check All ignore paging
            sql_chk_all = request.sql;
            connection.query(sql_chk_all, (error, results) => {
                if (error) {
                    pricelogger.error("failur of CurrentSql (Check-All):" + error.message);
                    throw error;
                }
                product_to_update_arr = results;
                //console.log(product_to_update_arr);
                from = "From Check All";

                if (product_to_update_arr.length) {
                    response_data = this.createUpdatedRow(connection, product_to_update_arr, request, from);
                    // console.log(response_data);
                    resultsCallback(response_data);
                }


            });
        } else {
            product_to_update_arr = request.selected_rows;
            from = "From Multiple Select";

            if (product_to_update_arr.length) {
                //console.log(response_data);
                response_data = this.createUpdatedRow(connection, product_to_update_arr, request, from);
            }
            //console.log(response_data);
            resultsCallback(response_data);
        }

    }//updateToBsOptions()

    roundValue(originalNumber, scale = 4) {
        if (isNaN(originalNumber) || isNaN(scale) || scale < 0) {
            return NaN; // Handle invalid inputs
        }

        const roundedNumber = Number(originalNumber.toFixed(scale));
        return roundedNumber;
    }

    bulkUpdateProducts(connection, type, data, log_type, update_type) {
        const chunk_size = 1000;
        let total_updated_records = 0;
        const chunk_data = Array.from({ length: Math.ceil(data.length / chunk_size) }).map(() => data.splice(0, chunk_size));
        //console.log(chunk_data);

        if (chunk_data.length) {

            //  console.log(chunk_data);
            for (const chunk_index in chunk_data) {
                let chunk_values = chunk_data[chunk_index]; //console.log(chunk_values.length);
                let all_col_data = [];
                let updated_product_ids = [];
                let all_history_data = [];
                let fields_changed = JSON.stringify(Array('new_selling_price'));

                if (type == "webshopprice") {
                    let sql = "INSERT INTO price_management_data (product_id, sku, selling_price, profit_percentage, profit_percentage_selling_price, percentage_increase, discount_on_gross_price) VALUES ";
                    let history_sql = "INSERT INTO price_management_history (product_id,old_net_unit_price,old_gross_unit_price,old_idealeverpakking,old_afwijkenidealeverpakking,old_buying_price,old_selling_price,new_net_unit_price,new_gross_unit_price,new_idealeverpakking,new_afwijkenidealeverpakking,new_buying_price,new_selling_price,updated_date_time,updated_by,is_viewed,fields_changed,buying_price_changed) VALUES ";

                    for (const key in chunk_values) {
                        let chunk_value = chunk_values[key];

                        //console.log(chunk_value);
                        all_col_data.push("('" + chunk_value.product_id + "', '" + chunk_value.sku + "', '" + chunk_value.selling_price + "', '" + chunk_value.profit_margin_bp + "', '" + chunk_value.profit_margin_sp + "', '" + chunk_value.percentage_increase + "', '" + chunk_value.discount_on_gross + "')");

                        // fields_changed.push("new_selling_price");
                        let buying_price_changed = 0;
                        all_history_data.push("('" + chunk_value.product_id + "', '" + chunk_value.webshop_net_unit_price + "', '" + chunk_value.old_gross_unit_price + "', '" + chunk_value.old_idealeverpakking + "', '" + chunk_value.old_afwijkenidealeverpakking + "', '" + chunk_value.old_buying_price + "', '" + chunk_value.old_selling_price + "','" + chunk_value.new_net_unit_price + "', '" + chunk_value.new_gross_unit_price + "', '" + chunk_value.new_idealeverpakking + "', '" + chunk_value.new_afwijkenidealeverpakking + "', '" + chunk_value.new_buying_price + "', '" + chunk_value.new_selling_price + "',now(),'Price Management','No','" + fields_changed + "','" + buying_price_changed + "')");
                        updated_product_ids.push(chunk_value.product_id);
                        total_updated_records++;
                    }

                    //console.log(all_history_data);

                    sql += all_col_data.join(", ") + " ON DUPLICATE KEY UPDATE selling_price = VALUES(selling_price),profit_percentage = VALUES(profit_percentage),profit_percentage_selling_price = VALUES(profit_percentage_selling_price),percentage_increase = VALUES(percentage_increase),discount_on_gross_price = VALUES(discount_on_gross_price)";
                    //console.log(sql);
                    connection.query(sql, (error, results) => {

                        if (error) {
                            pricelogger.error("bulkUpdateProducts Error (" + log_type + "):" + error.message);
                        } else {
                            history_sql += all_history_data.join(", ");
                            connection.query(history_sql, (error, results) => {
                                if (error) {
                                    pricelogger.error("History Update bulkUpdateProducts Error (" + log_type + "):" + error.message);
                                } else {
                                    pricelogger.info("Bulk Updated " + update_type + " Chunk (" + chunk_index + ") : " + chunk_values.length + " Records.");
                                    this.changeUpdateStatus(connection, updated_product_ids.join(','));
                                }
                            });
                        }
                    });
                }
            }
        }
        return total_updated_records;
    }//end bulkUpdateProducts()

    changeUpdateStatus(conn, product_id) {
        let change_status = "UPDATE price_management_data SET is_updated = '1' WHERE product_id IN (" + product_id + ")";
        conn.query(change_status, (error, results) => {
            if (error) {
                pricelogger.error("changeupdate status Error (" + change_status + "):" + error.message);
            }
        });
    }


    calculate_expression_sp(expression, v, subtype = 'bigshopper') {
        let new_selling_price = 0;
        let bs_percent = expression[0];
        let bs_more_less = expression[1];
        let bs_price_type = expression[2];
        if (subtype == 'bigshopper') {
            if (bs_more_less == 'more' && bs_price_type == 'bs_percent_hp') {
                new_selling_price = v[0].highest_price + ((bs_percent * v[0].highest_price) / 100);
            } else if (bs_more_less == 'more' && bs_price_type == 'bs_percent_lp') {
                new_selling_price = v[1].lowest_price + ((bs_percent * v[1].lowest_price) / 100);
            } else if (bs_more_less == 'less' && bs_price_type == 'bs_percent_lp') {
                new_selling_price = v[1].lowest_price - ((bs_percent * v[1].lowest_price) / 100);
            } else {
                new_selling_price = v[0].highest_price - ((bs_percent * v[0].highest_price) / 100);
            }
        } else {
            let calculate_percentage_np = (v[0].price_of_the_next_excl_shipping * bs_percent) / 100;
            if (bs_more_less == 'more') {
                //removed roundvalue
                new_selling_price = v[0].price_of_the_next_excl_shipping + calculate_percentage_np;
            } else {
                new_selling_price = v[0].price_of_the_next_excl_shipping - calculate_percentage_np;
            }
        }

        return new_selling_price;
    }

    createUpdatedRow(connection, product_to_update_arr, request, from) {
        let all_selected_data = [];
        let updated_product_ids = [];
        let in_sku_arr = [];
        let notify_this = 0;
        let response_data = [];
        response_data.push({ 'msg': 'No records available to update' });
        response_data.push({ 'type': 'info' });

        for (const row_index in product_to_update_arr) {
            const row = product_to_update_arr[row_index];
            /*console.log(row);
            console.log(row["bigshopper_highest_price"]);
            */
            let new_selling_price = 0.0000;
            let check_continue_1 = 0;
            let x = new Number(row["highest_price"]);
            let y = new Number(row["lowest_price"]);

            if (request.bs_price_option_checked == 'lowest_price') {
                //console.log(row["lowest_price"]);
                if (row["lowest_price"] == '---' || row["lowest_price"] == 0.0000) {
                    check_continue_1 = 1;
                    continue;
                } else {
                    new_selling_price = row["lowest_price"];
                    // console.log(new_selling_price);
                }
            } else if (request.bs_price_option_checked == 'highest_price') {
                if (row["highest_price"] == '---' || row["highest_price"] == 0.0000) {
                    check_continue_1 = 1;
                    continue;
                } else {
                    new_selling_price = row["highest_price"];
                }
            } else if (request.bs_price_option_checked == 'between_bs') {
                if ((row["lowest_price"] == '---' || row["lowest_price"] == 0.0000) && (row["highest_price"] == "---" || row["highest_price"] == 0.0000)) {
                    check_continue_1 = 1;
                    continue;
                } else {
                    new_selling_price = (x + y) / 2;
                }
            } else if (request.bs_price_option_checked == 'percentage_bs') {
                if (request.expression[2] == 'bs_percent_lp' && (row.lowest_price == "---" || row.lowest_price == 0.0000) && (request.expression[2] == 'bs_percent_hp' && row["highest_price"] == "---" || row["highest_price"] == 0.0000)) {
                    check_continue_1 = 1;
                    continue;
                } else {
                    let v = [];
                    v.push({ "highest_price": x });
                    v.push({ "lowest_price": y });
                    new_selling_price = this.calculate_expression_sp(request.expression, v);
                }
            } else if (request.bs_price_option_checked == 'equal_to_next_price') {
                if (row["price_of_the_next_excl_shipping"] == '---' || row["price_of_the_next_excl_shipping"] == 0.0000) {
                    check_continue_1 = 1;
                    continue;
                } else {
                    new_selling_price = row["price_of_the_next_excl_shipping"];
                }
            } else if (request.bs_price_option_checked == 'percent_next_price') {
                if (row["price_of_the_next_excl_shipping"] == '---' || row["price_of_the_next_excl_shipping"] == 0.0000) {
                    check_continue_1 = 1;
                    continue;
                } else {
                    let v = [];
                    let z = new Number(row["price_of_the_next_excl_shipping"]);
                    v.push({ "price_of_the_next_excl_shipping": z });
                    new_selling_price = this.calculate_expression_sp(request.expression, v, 'next_price');
                }
            }

            if (new_selling_price == row["selling_price"]) {
                continue;
            }
            if (new_selling_price <= row['buying_price']) {
                notify_this++;
                continue;
            }

            let webshop_selling_price = row["webshop_selling_price"];//gyzs_selling_price
            let pmd_buying_price = row["buying_price"];
            //let supplier_gross_price = (row["supplier_gross_price"] == 0 ? 1 : row["supplier_gross_price"]);
            let supplier_gross_price = (row["gross_unit_price"] == 0 ? 1 : row["gross_unit_price"]);

            //gross_unit_price
            let percentage_increase = this.roundValue(((new_selling_price - webshop_selling_price) / webshop_selling_price) * 100);
            let profit_margin = this.roundValue(((new_selling_price - pmd_buying_price) / pmd_buying_price) * 100);
            let profit_margin_sp = this.roundValue(((new_selling_price - pmd_buying_price) / new_selling_price) * 100);
            let discount_percentage = this.roundValue((1 - (new_selling_price / supplier_gross_price)) * 100);

            // set history fields and PMD fields to update
            all_selected_data.push({
                'selling_price': new_selling_price,
                'profit_margin_bp': profit_margin,
                'profit_margin_sp': profit_margin_sp,
                'discount_on_gross': discount_percentage,
                'percentage_increase': percentage_increase,
                'product_id': row["product_id"],
                'sku': row["sku"],
                'old_net_unit_price': row["webshop_net_unit_price"],
                'old_gross_unit_price': row["webshop_gross_unit_price"],
                'old_idealeverpakking': row["webshop_idealeverpakking"],
                'old_afwijkenidealeverpakking': row["webshop_afwijkenidealeverpakking"],
                'old_buying_price': row["webshop_buying_price"],
                'old_selling_price': row["selling_price"],
                'new_net_unit_price': pmd_buying_price,
                'new_gross_unit_price': row["gross_unit_price"],
                'new_idealeverpakking': row["idealeverpakking"],
                'new_afwijkenidealeverpakking': row["afwijkenidealeverpakking"],
                'new_buying_price': pmd_buying_price,
                'new_selling_price': new_selling_price,
                'webshop_net_unit_price': row["webshop_net_unit_price"]
            });

            updated_product_ids.push(row['product_id']);
            in_sku_arr[row['sku']] = row['sku'];

        }//end loop

        // console.log(all_selected_data);all_selected_data.length

        if (all_selected_data.length) {
            response_data.splice(0);
            let total_rec = this.bulkUpdateProducts(connection, "webshopprice", all_selected_data, from, "Selling Price");
            let updated_recs = [];
            updated_recs.push(total_rec);
            if (notify_this) {
                updated_recs.push(notify_this);
                response_data.push({ 'msg': updated_recs.join('_') });
            } else {
                response_data.push({ 'msg': "Updated " + updated_recs[0] + " records." });
            }
        } else {
            if (notify_this > 0) {
                response_data.splice(0);
                response_data.push({ 'msg': "Notify_" + notify_this });
            }
        }

        return response_data;
    }

}
module.exports = new productPriceService();