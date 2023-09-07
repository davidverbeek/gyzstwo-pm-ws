const express = require('express')
const fs = require('fs');
const app = express()
const port = 3200

app.get('/', (req, res) => {
  res.send('Hello World!')
})

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})

var cors = require('cors');
app.use(cors());

const bodyParser = require("body-parser");
var jwt = require('jsonwebtoken');
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: true, parameterLimit: 50000 }));



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
var bolCommissionService = require('./services/bolCommissionService');
var bolMinimumService = require('./services/bolMinimumService');
var revenueService = require('./services/revenueService');



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
    //console.log('The solution is: ', rows[0].solution)
    //res.json(err);
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
  productPriceService.savePriceData(connection, req.body, (msg) => {
    res.json({ msg });
  });
});
app.post('/upload-products', bodyParser.json(), function (req, res) {
  productPriceService.uploadPriceData(connection, req.body, (msg) => {
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


app.post('/get-products-byskus', bodyParser.json(), function (req, res) {
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
  connection.query("UPDATE pm_settings SET roas = '" + JSON.stringify(req.body) + "' WHERE id = 1", (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {

      fs.writeFile('../s.json', JSON.stringify(req.body), (err) => {
        if (err) throw err;
      });
      return res.status(200).json({ settings: JSON.stringify(req.body) });
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

