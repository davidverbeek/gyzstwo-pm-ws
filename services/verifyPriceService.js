//code to insert into db

//read all pmd then foreach through each row
// create new data
//insert into table
class verifyPriceService {
    verifyPricePercentage(connection, request, resultsCallback) {
        let offset = 0;

        connection.query("delete from verify_prices", (error, results, fields) => {
            if (error) {
                //resultsCallback('Verification failed!');
                console.error('Error emptying  table: ', error);
                return;
            }
            function fetchAndInsertChunk() {
                const chunk_size = 1000;
                let SQL = "SELECT pmd.product_id AS product_id,pmd.sku AS sku, pmd.buying_price AS buying_price,pmd.selling_price AS selling_price,pmd.gross_unit_price, pmd.profit_percentage AS profit_margin,profit_percentage_selling_price AS ppsp,discount_on_gross_price AS dgp FROM price_management_data AS pmd";

                //const chunkSize = 100; // Set the desired chunk size

                let processedRows = [];
                connection.query(SQL, (error, results, fields) => {
                    if (error) {
                        console.error('Error selecting data: ', error);
                        return;
                    }

                    /* if (results.length === 0) {
                        console.log('All records inserted.');
                        connection.end(); // Close the database connection
                        return;
                    } */
                    //$all_pm_data = array();
                    // Process the query results using forEach
                    results.forEach((row) => {
                        let supplier_gross_price = 1;
                        if (row.gross_unit_price == 0) {
                            supplier_gross_price = 1;
                        } else {
                            supplier_gross_price = row.gross_unit_price;
                        }
                        let to_verify_mbp = (((row.selling_price - row.buying_price) / row.buying_price) * 100).toFixed(4);
                        let to_verify_msp = (((row.selling_price - row.buying_price) / row.selling_price) * 100).toFixed(4);
                        let to_verify_dgp = ((1 - (row.selling_price / supplier_gross_price)) * 100).toFixed(4)
                        const verifiedRow = {
                            product_id: row.product_id,
                            buying_price: row.buying_price,
                            selling_price: row.selling_price,
                            gross_unit_price: row.gross_unit_price,
                            profit_percentage: row.profit_margin,
                            profit_percentage_selling_price: row.ppsp,
                            discount_on_gross_price: row.dgp,

                            verify_profit_percentage: to_verify_mbp,
                            verify_profit_percentage_sp: to_verify_msp,
                            verify_discount_on_gp: to_verify_dgp,
                            diff_profit_percentage_bp: row.profit_margin - to_verify_mbp,
                            diff_profit_percentage_sp: row.ppsp - to_verify_msp,
                            diff_discount_percentage_gp: row.dgp - to_verify_dgp
                            // Add more fields or processing as needed
                        };

                        processedRows.push(verifiedRow);
                    });
                    //  console.log(processedRows);
                    const chunks = Array.from({ length: Math.ceil(processedRows.length / chunk_size) }).map(() => processedRows.splice(0, chunk_size));
                    //  console.log(chunks);
                    /* temp connection.query("delete from verify_prices", (error, results, fields) => { */
                    /* temp if (error) {
                        //resultsCallback('Verification failed!');
                        console.error('Error emptying  table: ' + error);
                        return;
                    }*/
                    //let user_msg = user_msg;
                    for (const chunk_key in chunks) {
                        let value = chunks[chunk_key];

                        console.log(value);
                        const keys = Object.keys(value[0]); // Extract column names from the row object
                        // Extract corresponding values
                        const tableName = 'verify_prices';

                        let col_data = {};
                        const values = {};
                        for (const chunk_data in value) {

                            //values[] = value;
                            // col_data.push(chunk_data);
                        }

                        // console.log(col_data);

                        const query = `INSERT INTO ${tableName} (${keys.join(', ')}) VALUES ${value.map(value => `(${value.product_id}, ${value.buying_price}, ${value.selling_price}, ${value.gross_unit_price}, ${value.profit_percentage}, ${value.profit_percentage_selling_price}, ${value.discount_on_gross_price}, ${value.verify_profit_percentage}, ${value.verify_profit_percentage_sp}, ${value.verify_discount_on_gp}, ${value.diff_profit_percentage_bp}, ${value.diff_profit_percentage_sp}, ${value.diff_discount_percentage_gp})`).join(', ')} ON DUPLICATE KEY UPDATE ${keys.map((key) => `${key} = VALUES(${key})`).join(', ')}`;



                        console.log(query);
                        connection.query(query, (insertError, results) => {
                            if (insertError) {
                                console.error('Error inserting  data: ', insertError);
                            } else {
                                //${results.length}
                                console.log(`Inserted Chunk no.:` + chunk_key + ` successfully.`);
                            }


                        });

                    }

                    /* processedRows.forEach((row) => {
                        const keys = Object.keys(row); // Extract column names from the row object
                        const values = Object.values(row); // Extract corresponding values
                        const tableName = 'verify_prices';

                        // Construct the dynamic query string
                        const query = `INSERT INTO ${ tableName } (${ keys.join(', ') })
                VALUES(${ values.map(() => '?').join(', ') })
                  ON DUPLICATE KEY UPDATE ${ keys.map((key) => `${key} = VALUES(${key})`).join(', ') } `;

                        // Execute the query
                        connection.query(query, values, (insertError, results) => {
                            if (error) {
                                console.error('Error inserting  data: ', insertError);
                            }
                        });
                    }); */



                    // offset += chunkSize;
                    //fetchAndInsertChunk(); // Continue fetching and inserting the next chunk
                    // }
                    //resultsCallback('Verification is done successfully!');

                    /* to take this  }); */
                    // console.log(user_msg);

                });
            }
            fetchAndInsertChunk();
        });
    }

    getData(connection, request, resultsCallback) {
        const SQL = this.buildSql(request); console.log(SQL);
        connection.query(SQL, (error, results) => {
            if (error) {
                console.error(error + SQL);
                return;
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

        const fromSql = " FROM price_management_data AS pmd INNER JOIN verify_prices AS vp ON vp.product_id = pmd.product_id";
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


    createSelectSql(request) {
        const rowGroupCols = request.rowGroupCols;
        const valueCols = request.valueCols;
        const groupKeys = request.groupKeys;

        /*  if (this.isDoingGrouping(rowGroupCols, groupKeys)) {
             const colsToSelect = [];
     
             const rowGroupCol = rowGroupCols[groupKeys.length];
             colsToSelect.push(rowGroupCol.field);
     
             valueCols.forEach(function (valueCol) {
                 colsToSelect.push(valueCol.aggFunc + '(' + valueCol.field + ') as ' + valueCol.field);
             });
     
             return ' select ' + colsToSelect.join(', ');
         } */


        return ' select DISTINCT vp.product_id,  pmd.sku, vp.gross_unit_price,vp.buying_price, vp.selling_price, vp.profit_percentage, vp.profit_percentage_selling_price, vp.discount_on_gross_price, vp.verify_profit_percentage, vp.verify_profit_percentage_sp, vp.verify_discount_on_gp,vp.diff_profit_percentage_bp,vp.diff_profit_percentage_sp,vp.diff_discount_percentage_gp,(SELECT COUNT(*) AS mag_updated_product_cnt FROM price_management_history WHERE product_id = pmd.product_id and is_viewed = "No" and updated_by = "Magento" and buying_price_changed = "1") AS mag_updated_product_cnt';
    }

    createLimitSql(request) {
        const startRow = request.startRow;
        const endRow = request.endRow;
        const pageSize = endRow - startRow;
        return ' limit ' + (pageSize + 1) + ' offset ' + startRow;
        //return ' limit 300';
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
                if (key == 'sku') {
                    whereParts.push(that.createFilterSql('pmd.' + key, item));

                } else {
                    whereParts.push(that.createFilterSql('vp.' + key, item));

                }
            });
            //console.log(whereParts);
        }

        var whereClause = "";

        if (whereParts.length > 0) {
            whereClause = whereParts.join(' AND ');
            whereClause += ' AND';
        }
        /*    if ((request.cats).length > 0) {
               whereClause += ' pmcp.category_id IN (' + request.cats + ') AND';
           } */
        if (whereClause == "") {
            return '';
        } else {
            return ' where ' + whereClause.replace(/ AND$/, '') + '';
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

    buildsqlcount(fromSql, whereSql) {
        var countsql = "";
        countsql = "SELECT COUNT(*) AS TOTAL_RECORDS " + fromSql + " " + whereSql + "";
        //console.log(countsql);
        return countsql;
    }


    isDoingGrouping(rowGroupCols, groupKeys) {
        // we are not doing grouping if at the lowest level. we are at the lowest level
        // if we are grouping by more columns than we have keys for (that means the user
        // has not expanded a lowest level group, OR we are not grouping at all).
        return rowGroupCols.length > groupKeys.length;
    }

    createFilterSql(key, item) {
        //console.log(key + "===" + item + "===" + item.filterType);

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

    getDataAndInsert(connection, request, resultsCallback) {
        connection.connect((err) => {
            if (err) {
                console.error('Error connecting to the database:', err);
                return;
            }

            console.log('Connected to the database');

            const sourceTable = 'price_management_data'; // Replace with the name of your source table
            const destinationTable = 'verify_prices'; // Replace with the name of your destination table
            const chunkSize = 1000; // Set the desired chunk size

            let offset = 0;

            function fetchAndInsertChunk() {

                const selectQuery = "SELECT pmd.product_id AS product_id,pmd.sku AS sku, pmd.buying_price AS buying_price,pmd.selling_price AS selling_price,pmd.gross_unit_price, pmd.profit_percentage AS profit_margin,profit_percentage_selling_price AS ppsp,discount_on_gross_price AS dgp FROM price_management_data AS pmd LIMIT ${offset}, ${chunkSize}";

                const insertQuery = `INSERT INTO ${destinationTable} SELECT * FROM ${sourceTable} LIMIT ${offset}, ${chunkSize} `;

                connection.query(selectQuery, (selectError, results) => {
                    if (selectError) {
                        console.error('Error selecting data:', selectError);
                        return;
                    }

                    if (results.length === 0) {
                        console.log('All records inserted.');
                        connection.end(); // Close the database connection
                        return;
                    }

                    connection.query(insertQuery, (insertError) => {
                        console.log(insertQuery);
                        if (insertError) {
                            console.error('Error inserting data:', insertError);
                        } else {
                            console.log(`Inserted ${results.length} records.`);
                            offset += chunkSize;
                            fetchAndInsertChunk(); // Continue fetching and inserting the next chunk
                        }
                    });
                });
            }

            // Start the process
            fetchAndInsertChunk();
        });

    }




}

module.exports = new verifyPriceService();