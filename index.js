const express = require('express')
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
var debterRuleFileService = require('./services/debterRuleService');

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

app.post('/save-debter-rules', bodyParser.json(), function (req, res) {
  debterRuleFileService.insertDebterRules(connection, req.body, (msg) => {
    return res.status(200).json({ msg });
  });
});

app.post('/catpro-products', bodyParser.json(), function (req, res) {
  console.log(req.body);
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