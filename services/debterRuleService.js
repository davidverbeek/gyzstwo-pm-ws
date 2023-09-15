class debterRuleService {

    getData(connection, request, resultsCallback) {
        const SQL = this.buildSql(request);
        connection.query(SQL, (error, results) => {
            // const rowCount = this.getRowCount(request, results);
            // const resultsForPage = this.cutResultsToPageSize(request, results);
            const currentSql = SQL;
            resultsCallback(resultsForPage, rowCount, currentSql);
        });
    }

    resetDebterPrices(connection, request, resultsCallback) {
        var debter_col_1 = "group_" + request.customer_group + "_debter_selling_price";
        var debter_col_2 = "group_" + request.customer_group + "_margin_on_buying_price";
        var debter_col_3 = "group_" + request.customer_group + "_margin_on_selling_price";
        var debter_col_4 = "group_" + request.customer_group + "_discount_on_grossprice_b_on_deb_selling_price";
        var sql_reset_price = "UPDATE price_management_data SET " + debter_col_1 + "=0, " + debter_col_2 + "=0, " + debter_col_3 + "=0, " + debter_col_4 + " = 0";
        sql_reset_price += " WHERE product_id IN (" + request.product_ids + ")";

        console.log(sql_reset_price);

        connection.query(sql_reset_price, (error, results) => {
            if (error) {
                return console.error(error.message);
            } else {
                return ('done');
                var products_arr = request.product_ids.split(",");
                console.log(products_arr.length + " products price has been reset");
            }

        });

    }

    resetDebterPrices_2(connection, request) {
        var debter_col_1 = "group_" + request.customer_group + "_debter_selling_price";
        var debter_col_2 = "group_" + request.customer_group + "_margin_on_buying_price";
        var debter_col_3 = "group_" + request.customer_group + "_margin_on_selling_price";
        var debter_col_4 = "group_" + request.customer_group + "_discount_on_grossprice_b_on_deb_selling_price";
        var sql_reset_price = "UPDATE price_management_data SET " + debter_col_1 + "=0, " + debter_col_2 + "=0, " + debter_col_3 + "=0, " + debter_col_4 + " = 0";
        sql_reset_price += " WHERE product_id IN (" + request.product_ids + ")";

        console.log(sql_reset_price);

        connection.query(sql_reset_price, (error, results) => {
            if (error) {
                return console.error(error.message);
            } else {
                // resultsCallback('done');
                var products_arr = request.product_ids.split(",");
                console.log(products_arr.length + " products price has been reset");
            }

        });

    }

    buildSql(request, totalrecords = "") {
        const selectSql = this.createSelectSql(request);
        const fromSql = " FROM price_management_catpro AS catpro ";
        const whereSql = this.createWhereSql(request);
        const SQL = selectSql + fromSql + whereSql;
        return SQL;
    }

    createSelectSql(request) {
        return ' select product_id';
    }

    createWhereSql(request) {

        return 'where category_id IN (' + request.cats + ')';

    }

    insertDebterRules(connection, request, resultsCallback) {

        let nodeDate = require('date-and-time');
        let now = nodeDate.format(new Date(), 'YYYY-MM-DD, HH:mm:ss');
        var sql = "INSERT INTO price_management_debter_categories(category_ids,product_ids,customer_group, updated_at) VALUES ";
        var all_col_data = "('" + request.category_ids + "', '" + request.product_ids + "','" + request.customer_group + "' ,'" + now + "')";
        sql += all_col_data;
        sql += " ON DUPLICATE KEY UPDATE category_ids = VALUES(category_ids), customer_group = VALUES(customer_group), product_ids = VALUES(product_ids), updated_at = VALUES(updated_at)";
        connection.query(sql, (error, results) => {
            if (error) {
                return console.error(error.message);
            } else {

                console.log("1 record inserted");
                return ('done');
            }
        });
    }


    getData_debter_rules(connection, request, resultsCallback) {
        let SQL = "";
        if (request.debter_id.split(',').length == 1) {
            SQL = this.buildSql_debter_rules(request);
        } else {
            SQL = this.buildSql_debter_rules(request, "multiple");
        }
        connection.query(SQL, (error, results) => {
            resultsCallback(error, results);
        });
    }


    buildSql_debter_rules(request, totalrecords = "") {
        let selectSql = this.createSelectSql_debter_rules(request);
        let whereSql = "";
        let fromSql = " FROM price_management_debter_categories AS debcats ";
        if (totalrecords == "") {
            whereSql = this.createWhereSql_debter_rules(request);
        } else {
            whereSql = this.createWhereSql_Multiple_debters(request);
        }
        const SQL = selectSql + fromSql + whereSql;
        return SQL;
    }

    createSelectSql_debter_rules(request) {
        return ' select category_ids';
    }

    createWhereSql_debter_rules(request) { //+ request.body +

        return 'where customer_group = "' + request.debter_id + '"';
    }

    createWhereSql_Multiple_debters(request) { //+ request.body +
        return 'where customer_group IN(' + request.debter_id + ')';
    }

    getRowCount_debter_rules(request, results) {
        if (results === null || results === undefined || results.length === 0) {
            return null;
        }
        const currentLastRow = request.startRow + results.length;
        return currentLastRow <= request.endRow ? currentLastRow : -1;
    }

    getCategoriesByAlias(connection, request, resultsCallback) {
        var result = Object.keys(request).map(function (k) { return request[k] }).join("', '");

        connection.query("SELECT cg.group_alias, dc.category_ids  FROM price_management_customer_groups as cg inner join price_management_debter_categories as dc on cg.magento_id = dc.customer_group where cg.group_alias IN ('" + result + "') and dc.category_ids != ''", function (err, result, fields) {
            if (err) throw err;
            //console.log(result);
            resultsCallback(err, result);
        });

    }//end getCategoriesByAlias()


    copyGroups(connection, request, resultsCallback) {
        try {
            //get destination and source categories
            //console.log(request);
            let msg = "";
            var SQL = "SELECT category_ids, product_ids FROM price_management_debter_categories dc where customer_group =" + request.source_group_id;
            connection.query(SQL, (err, result, fields) => {
                if (err) throw err;

                var new_cat_arr = "";
                let arr2 = new_cat_arr;
                var new_product_ids = "";

                if (result[0]) {
                    new_cat_arr = result[0].category_ids;
                    // console.log(new_cat_arr);
                    arr2 = new_cat_arr.split(',');
                    new_product_ids = result[0].product_ids;
                }

                let arr1 = [];
                var old_product_ids = "";
                var old_cat_arr = "";
                var destination_group_name = "";
                let destination_group = "";

                var SQL_2 = "select dc.category_ids, dc.product_ids, cg.customer_group_name, cg.magento_id from price_management_customer_groups as cg  LEFT JOIN  price_management_debter_categories AS dc ON dc.customer_group=cg.magento_id  WHERE cg.magento_id =" + request.destination_group_id;
                connection.query(SQL_2, (err_2, result_2, destination_group) => {
                    //console.log(result_2);
                    if (result_2.length) {


                        old_cat_arr = result_2[0].category_ids;
                        if (old_cat_arr != null) {
                            old_product_ids = result_2[0].product_ids;
                            arr1 = old_cat_arr.split(',');
                        }

                        destination_group_name = result_2[0].customer_group_name;
                        destination_group = result_2[0].magento_id;

                        //check to reset prices that old cats are removed or not
                        let difference = arr1.filter(x => !arr2.includes(x));
                        if (arr1.length > 0 && difference.length > 0) {
                            this.resetDebterPrices(connection, { customer_group: destination_group_name, product_ids: old_product_ids }, msg);
                        }
                    }
                    // update query in debter categories
                    let a = {
                        'category_ids': new_cat_arr,
                        'product_ids': new_product_ids,
                        'customer_group': destination_group,
                    }
                    this.insertDebterRules(connection, a, msg);
                });
            })
        } catch (e) {
            console.log("Name of Error: ", e.name);
            console.log("Description about Error: ", e.message);
        }
        resultsCallback("Data is copied and prices are reset successfully.");

        //result is single row
        //console.log(result);

    }//end copyGroups()

    /**
     * function to get list of debtors "whose categories are copied from. It is a source debtor in copy-categories of debter rules" 
     * @param {*} connection
     * @param {*} request
     * @param {*} resultsCallbacks
     */
    getListCopyDebtors(connection, request, resultsCallback) {
        const SQL = "SELECT cg.magento_id, cg.group_alias FROM gyzs_admin_management.price_management_debter_categories dc JOIN price_management_customer_groups cg ON dc.customer_group=cg.magento_id ORDER BY cg.customer_group_name";
        connection.query(SQL, (err, result) => {
            resultsCallback(err, result);
        });
    }

    getAllDebtorProduct(connection, request, resultsCallback) {

        const SQL = "SELECT * FROM price_management_customer_groups JOIN price_management_debter_categories ON price_management_debter_categories.customer_group = price_management_customer_groups.magento_id";
        connection.query(SQL, (err, result) => {
            console.log(err);
            resultsCallback(result);
        });
    }

}

module.exports = new debterRuleService();