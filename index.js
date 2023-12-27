const express = require('express')
const fs = require('fs');
const app = express()
const port = 3200

const multer = require('multer');
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, 'googleroas')
  },
  filename: function (req, file, cb) {
    cb(null, file.originalname)
  }
})
const upload = multer({ storage: storage })
var cors = require('cors');
app.use(cors());

const server = app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
});

envConfig = require("./env");
/* const io = require('socket.io')(server, {
  cors: {
    origins: ["*"]
  }
}); */

const bodyParser = require("body-parser");
var jwt = require('jsonwebtoken');
app.use(bodyParser.json({ limit: '50000mb' }));
app.use(bodyParser.urlencoded({ limit: '50000mb', extended: true, parameterLimit: 500000 }));

dbconfig = require("./dbconfig");
const mysql = require('mysql')
const connection = mysql.createConnection({
  host: dbconfig.host,
  user: dbconfig.user,
  password: dbconfig.password,
  database: dbconfig.database
})
connection.connect()

var productPriceService = require('./services/productPriceService');
var productPriceHistoryService = require('./services/productPriceHistoryService');
var debterRuleFileService = require('./services/debterRuleService');
var bolCommissionService = require('./services/bolCommissionService');
var bolMinimumService = require('./services/bolMinimumService');
var revenueService = require('./services/revenueService');
var orderService = require('./services/orderService');
var currentRoasService = require('./services/currentRoasService');
var googleActualRoasService = require('./services/googleActualRoasService');

app.post('/uploadgoogleroas', upload.single('googleroas'), function (req, res, next) {
  // req.file is the `avatar` file
  // req.body will hold the text fields, if there were any

  connection.query("INSERT INTO google_actual_roas SET from_date = '" + req.body.startdate + "', to_date = '" + req.body.enddate + "', file_name = '" + req.file.filename + "'");
  res.send({
    success: true,
    message: "File uploaded"
  })
})

app.post('/auth', bodyParser.json(), (req, res) => {
  // res.json(req.body);
  connection.query('SELECT count(*) AS count_user, page_access FROM users WHERE username = "' + req.body.username + '" AND password = "' + req.body.password + '"', (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {
      if (rows[0].count_user == 1) {
        var token = jwt.sign({ username: req.body.user_name, page_access: rows[0].page_access }, 'my_secret');
        return res.status(200).json({ token: token });
      } else {
        return res.status(501).json({ message: "Invalid user" });
      }
    }
  })
})

app.post('/pm-products', bodyParser.json(), function (req, res) {
  productPriceService.getData(connection, req.body, (rows, lastRow, currentSql) => {
    productPriceService.getDataCount(connection, req.body, (recordCount) => {
      if (lastRow == "-1") {
        lastRow = recordCount;
      }
      res.json({ rows: rows, lastRow: lastRow, currentSql: currentSql });
    })
  });
});

app.post('/save-products', bodyParser.json(), function (req, res) {
  //console.log(req.body);
  productPriceService.savePriceData(connection, req.body, (msg) => {
    res.json({ msg });
  });
});

//io.on('connection', socket => {
/* setInterval(() => {
  io.emit("showUploadProgress", Math.random());
}, 1000); */
//});


app.post('/upload-products', bodyParser.json(), function (req, res) {
  /* productPriceService.uploadPriceData(connection, req.body, io, (msg) => {
    res.json({ msg });
  }); */
  productPriceService.uploadPriceData(connection, req.body, fs, envConfig.pmSettingsJsonPath, (msg) => {
    res.json({ msg });
  });
});


app.post('/save-bol-delivery-time', bodyParser.json(), function (req, res) {
  bolMinimumService.saveBolDeliveryTime(connection, req.body, (msg) => {
    res.json({ msg });
  });
});

app.post('/save-bol-delivery-time-be', bodyParser.json(), function (req, res) {
  bolMinimumService.saveBolDeliveryTimeBE(connection, req.body, (msg) => {
    res.json({ msg });
  });
});

app.post('/all-products', bodyParser.text(), function (req, res) {
  productPriceService.getAllProducts(connection, req.body, (msg) => {
    res.json({ msg });
  });
});

app.post('/all-revenue', bodyParser.text(), function (req, res) {
  revenueService.getAllRevenue(connection, req.body, (msg) => {
    res.json({ msg });
  });
});

app.get('/all-debtors', bodyParser.json(), function (req, res) {
  productPriceService.getAllDebtors(connection, req.body, (msg) => {
    res.send({ msg });
  });
});

app.get('/get-ec-deliverytimes', bodyParser.json(), function (req, res) {
  bolMinimumService.getAllECDeliveryTimes(connection, req.body, (msg) => {
    res.send({ msg });
  });
});

app.get('/get-ec-deliverytimes-be', bodyParser.json(), function (req, res) {
  bolMinimumService.getAllECDeliveryTimesBE(connection, req.body, (msg) => {
    res.send({ msg });
  });
});


app.get('/get-products-byskus', bodyParser.json(), function (req, res) {
  productPriceService.getAllProductBySkus(connection, req.body, (msg) => {
    res.send({ msg });
  });
});

app.get('/activate-updated-products', bodyParser.json(), function (req, res) {
  productPriceService.activateUpdatedProducts(connection, req.body, (msg) => {
    res.send({ msg });
  });
});

app.get('/all-categories', bodyParser.json(), function (req, res) {
  connection.query("SELECT id,pid,name FROM price_management_ctree", (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {
      return res.status(200).json({ categories: rows });
    }
  })
});

app.post('/verifytoken', bodyParser.json(), (req, res) => {
  jwt.verify(req.body.token, "my_secret", function (err, tokendata) {
    if (err) {
      res.status(400).json({ message: "Unauthorized request" });
    } else {
      res.status(200).json(tokendata);
    }
  });
});

app.post('/pm-products-history', bodyParser.json(), function (req, res) {
  productPriceHistoryService.getData(connection, req.body, (rows, lastRow, currentSql) => {
    productPriceHistoryService.getDataCount(connection, req.body, (recordCount) => {
      if (lastRow == "-1") {
        lastRow = recordCount;
      }
      res.json({ rows: rows, lastRow: lastRow, currentSql: currentSql });
    })
  });
});

app.post('/pm-order-history', bodyParser.json(), function (req, res) {
  orderService.getData(connection, req.body, (rows, lastRow, currentSql) => {
    orderService.getDataCount(connection, req.body, (recordCount) => {
      if (lastRow == "-1") {
        lastRow = recordCount;
      }
      res.json({ rows: rows, lastRow: lastRow, currentSql: currentSql });
    })
  });
});



app.post('/pm-bol-commission', bodyParser.json(), function (req, res) {
  bolCommissionService.getData(connection, req.body, (rows, lastRow, currentSql) => {
    bolCommissionService.getDataCount(connection, req.body, (recordCount) => {
      if (lastRow == "-1") {
        lastRow = recordCount;
      }
      res.json({ rows: rows, lastRow: lastRow, currentSql: currentSql });
    })
  });
});

app.post('/pm-bol-minimum', bodyParser.json(), function (req, res) {
  bolMinimumService.getData(connection, req.body, (rows, lastRow, currentSql) => {
    bolMinimumService.getDataCount(connection, req.body, (recordCount) => {
      if (lastRow == "-1") {
        lastRow = recordCount;
      }
      res.json({ rows: rows, lastRow: lastRow, currentSql: currentSql });
    })
  });
});

app.post('/save-debter-rules', bodyParser.json(), function (req, res) {
  debterRuleFileService.insertDebterRules(connection, req.body, (msg) => {
    return res.status(200).json({ msg });
  });
});

app.post('/catpro-products', bodyParser.json(), function (req, res) {
  connection.query("SELECT distinct product_id FROM price_management_catpro where category_id IN (" + req.body + ")", (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' + err });
    } else {
      return res.status(200).json({ products_of_cats: rows });
    }
  })
});

app.post('/dbt-rules-cats', bodyParser.json(), function (req, res) {
  // console.log(req);
  debterRuleFileService.getData_debter_rules(connection, req.body, (err, rows) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' + err });
    } else {
      return res.status(200).json({ debter_cats: rows });
    }
  });
});

app.post('/dbt-rules-reset', bodyParser.json(), function (req, res) {
  debterRuleFileService.resetDebterPrices(connection, req.body, (msg) => {
    res.json({ msg });
  });

});

app.post('/category-brand', bodyParser.json(), function (req, res) {
  productPriceService.getCategoryBrand(connection, req.body, (brands_of_cats) => {
    res.send({ brands_of_cats });
  });
});

app.get('/all-debtor-product', bodyParser.json(), function (req, res) {
  debterRuleFileService.getAllDebtorProduct(connection, req.body, (msg) => {
    res.send({ msg });
  });
});

app.post('/dbt-alias-cats', bodyParser.json(), function (req, res) {
  debterRuleFileService.getCategoriesByAlias(connection, req.body, (rows) => {
    res.json({ rows });
  });
});

app.get('/list-copy-debtors', bodyParser.json(), function (req, res) {
  debterRuleFileService.getListCopyDebtors(connection, req.body, (rows) => {
    res.send({ rows });
  });
});

app.get('/get-settings', bodyParser.json(), function (req, res) {
  connection.query("SELECT roas FROM pm_settings WHERE id = 1", (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {
      return res.status(200).json({ settings: rows });
    }
  })
});

app.post('/set-settings', bodyParser.json(), function (req, res) {

  var simpleFields = req.body[0];
  var setroasFields = req.body[1];
  var employeecostFields = req.body[2];


  roas_settings = {};
  roas_lb = {};
  roas_ub = {};

  employeecost_lb = {};
  employeecost_ub = {};



  shipment_revenue = {};

  shipment_revenue["peak_order_value"] = {};
  shipment_revenue["transmission"] = {};
  shipment_revenue["transmission"]["transmission_shippment_revenue_less_then"] = {};
  shipment_revenue["transmission"]["transmission_shippment_revenue_greater_then_or_equal"] = {};

  shipment_revenue["other"] = {};
  shipment_revenue["other"]["other_shippment_revenue_less_then"] = {};
  shipment_revenue["other"]["other_shippment_revenue_greater_then_or_equal"] = {};


  for (const fieldname in simpleFields) {
    roas_settings["transmission_shipping_cost"] = simpleFields["transmission_shipping_cost"];
    roas_settings["transmission_packing_cost"] = simpleFields["transmission_packing_cost"];
    roas_settings["transmission_extra_return_shipment_cost"] = simpleFields["transmission_extra_return_shipment_cost"];
    roas_settings["pakketpost_shipping_cost"] = simpleFields["pakketpost_shipping_cost"];
    roas_settings["pakketpost_packing_cost"] = simpleFields["pakketpost_packing_cost"];
    roas_settings["pakketpost_extra_return_shipment_cost"] = simpleFields["pakketpost_extra_return_shipment_cost"];
    roas_settings["briefpost_shipping_cost"] = simpleFields["briefpost_shipping_cost"];
    roas_settings["briefpost_packing_cost"] = simpleFields["briefpost_packing_cost"];
    roas_settings["briefpost_extra_return_shipment_cost"] = simpleFields["briefpost_extra_return_shipment_cost"];
    roas_settings["bol_commissions_auth_url"] = simpleFields["bol_commissions_auth_url"];
    roas_settings["bol_client_id"] = simpleFields["bol_client_id"];
    roas_settings["bol_secret"] = simpleFields["bol_secret"];
    roas_settings["bol_commissions_api_url"] = simpleFields["bol_commissions_api_url"];
    roas_settings["bol_buying_percentage"] = simpleFields["bol_buying_percentage"];
    roas_settings["bol_return_from_date"] = simpleFields["bol_return_from_date"];
    roas_settings["bol_return_to_date"] = simpleFields["bol_return_to_date"];

    roas_lb[simpleFields["roasval_lb_set_option"]] = simpleFields["roasval_lb_set_value"];
    roas_settings["roas_lower_bound"] = roas_lb;
    roas_ub[simpleFields["roasval_ub_set_option"]] = simpleFields["roasval_ub_set_value"];
    roas_settings["roas_upper_bound"] = roas_ub;

    shipment_revenue["peak_order_value"] = simpleFields["shippment_revenue_order_value_peak"];
    shipment_revenue["transmission"]["transmission_shippment_revenue_less_then"] = simpleFields["transmission_shippment_revenue_less_then"];
    shipment_revenue["transmission"]["transmission_shippment_revenue_greater_then_or_equal"] = simpleFields["transmission_shippment_revenue_greater_then_or_equal"];
    shipment_revenue["other"]["other_shippment_revenue_less_then"] = simpleFields["other_shippment_revenue_less_then"];
    shipment_revenue["other"]["other_shippment_revenue_greater_then_or_equal"] = simpleFields["other_shippment_revenue_greater_then_or_equal"];
    roas_settings["shipment_revenue"] = shipment_revenue;

    employeecost_lb[simpleFields["empcost_ov_lb_set_option"]] = simpleFields["empcost_lb_set_value"];
    roas_settings["employeecost_lower_bound"] = employeecost_lb;
    employeecost_ub[simpleFields["empcost_ov_ub_set_option"]] = simpleFields["empcost_ub_set_value"];
    roas_settings["employeecost_upper_bound"] = employeecost_ub;
    roas_settings["avg_order_per_month"] = simpleFields["avg_order_per_month"];
    roas_settings["payment_cost"] = simpleFields["payment_cost"];
    roas_settings["other_company_cost"] = simpleFields["other_company_cost"];
    roas_settings["individual_sku_percentage"] = simpleFields["individual_sku_percentage"];
    roas_settings["category_brand_percentage"] = simpleFields["category_brand_percentage"];
    roas_settings["sku_afzet_period"] = simpleFields["sku_afzet_period"];

    if (simpleFields["excludeBol"] === true) {
      roas_settings["excludeBol"] = 1;
    } else {
      roas_settings["excludeBol"] = 0;
    }
  }

  const roasranges = undefined || {};
  for (let roasField in Object.keys(setroasFields)) {
    for (let key in setroasFields[roasField]) {
      const roasrangeobject = undefined || {};
      roasrangeobject["r_val"] = setroasFields[roasField]["roasval"];
      if (setroasFields[roasField]["roasvaltype"] === true) {
        roasrangeobject["r_type"] = "fixed";
      } else {
        roasrangeobject["r_type"] = "increment";
      }
      roasranges[setroasFields[roasField]["roasmin"] + "-" + setroasFields[roasField]["roasmax"]] = roasrangeobject;
    }
  }

  const employeecostranges = undefined || {};
  for (let employeecostField in Object.keys(employeecostFields)) {
    for (let ekey in employeecostFields[employeecostField]) {
      employeecostranges[employeecostFields[employeecostField]["empcost_ov_min"] + "-" + employeecostFields[employeecostField]["empcost_ov_max"]] = employeecostFields[employeecostField]["empcostval"];
    }
  }

  roas_settings["roas_range"] = roasranges;
  roas_settings["employeecost_range"] = employeecostranges;
  connection.query("UPDATE pm_settings SET roas = '" + JSON.stringify(roas_settings) + "' WHERE id = 1", (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {

      fs.writeFile(envConfig.pmSettingsJsonPath + '/s.json', JSON.stringify(roas_settings), (err) => {
        if (err) throw err;
      });
      return res.status(200).json({ settings: JSON.stringify(roas_settings) });
    }
  })
});

app.post('/pm-revenue', bodyParser.json(), function (req, res) {
  revenueService.getData(connection, req.body, (rows, lastRow, currentSql) => {
    revenueService.getDataCount(connection, req.body, (recordCount) => {
      if (lastRow == "-1") {
        lastRow = recordCount;
      }
      res.json({ rows: rows, lastRow: lastRow, currentSql: currentSql });
    })
  });
});

app.post('/currentroas', bodyParser.json(), function (req, res) {
  currentRoasService.getData(connection, req.body, (rows, lastRow, currentSql) => {
    currentRoasService.getDataCount(connection, req.body, (recordCount) => {
      if (lastRow == "-1") {
        lastRow = recordCount;
      }
      res.json({ rows: rows, lastRow: lastRow, currentSql: currentSql });
    })
  });
});

app.get('/get-pm-revenue', bodyParser.json(), function (req, res) {
  connection.query("SELECT * FROM gyzsrevenuedata ORDER BY sku_vericale_som DESC LIMIT 1", (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {
      return res.status(200).json({ revenueData: rows });
    }
  })
});

app.get('/get-pm-revenue-sum', bodyParser.json(), function (req, res) {
  connection.query("SELECT SUM(sku_refund_revenue_amount) AS tot_refund_amount, SUM(sku_abs_margin) AS tot_abs_margin FROM gyzsrevenuedata", (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {
      return res.status(200).json({ revenueSumData: rows });
    }
  })
});

app.post('/set-roasdate', bodyParser.json(), function (req, res) {
  connection.query("UPDATE roas_date SET new_roas_feed_from_date = '" + req.body[0] + "', new_roas_feed_to_date = '" + req.body[1] + "' WHERE id = 1", (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {
      return res.status(200).json({ message: 'success' });
    }
  })
});

app.get('/get-roasdate', bodyParser.json(), function (req, res) {
  connection.query("Select * FROM roas_date WHERE id = 1", (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {
      return res.status(200).json({ message: rows });
    }
  })
});

app.post('/all-roas', bodyParser.text(), function (req, res) {
  currentRoasService.getAllRoas(connection, req.body, (msg) => {
    res.json({ msg });
  });
});

app.post('/google-actual-roas', bodyParser.json(), function (req, res) {
  googleActualRoasService.getData(connection, req.body, (rows, lastRow, currentSql) => {
    googleActualRoasService.getDataCount(connection, req.body, (recordCount) => {
      if (lastRow == "-1") {
        lastRow = recordCount;
      }
      res.json({ rows: rows, lastRow: lastRow, currentSql: currentSql });
    })
  });
});

app.post('/save-google-actual-roas', bodyParser.json(), function (req, res) {
  googleActualRoasService.saveGoogleRoas(connection, req.body, (msg) => {
    res.json({ msg });
  });
});

app.post('/copy-debters', bodyParser.json(), function (req, res) {
  debterRuleFileService.copyGroups(connection, req.body, (msg) => {
    res.json({ msg: msg });
  });
});