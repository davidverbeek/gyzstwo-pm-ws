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
                resultsCallback('done');
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
                resultsCallback('done');
                console.log("1 record inserted");
            }

        });

    }

    getData_debter_rules(connection, request, resultsCallback) {
        const SQL = this.buildSql_debter_rules(request);
        connection.query(SQL, (error, results) => {
            resultsCallback(error, results);
        });
    }


    buildSql_debter_rules(request, totalrecords = "") {
        const selectSql = this.createSelectSql_debter_rules(request);
        const fromSql = " FROM price_management_debter_categories AS debcats ";
        const whereSql = this.createWhereSql_debter_rules(request);
        const SQL = selectSql + fromSql + whereSql;
        return SQL;
    }

    createSelectSql_debter_rules(request) {
        return ' select category_ids';
    }

    createWhereSql_debter_rules(request) { //+ request.body +

        return 'where customer_group = "' + request.debter_id + '"';
    }

    getRowCount_debter_rules(request, results) {
        if (results === null || results === undefined || results.length === 0) {
            return null;
        }
        const currentLastRow = request.startRow + results.length;
        return currentLastRow <= request.endRow ? currentLastRow : -1;
    }


}

module.exports = new debterRuleService();