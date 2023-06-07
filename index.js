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

app.post('/auth', bodyParser.json(), (req, res) => {
  // res.json(req.body);
  connection.query('SELECT count(*) AS count_user, page_access FROM users WHERE username = "' + req.body.username + '" AND password = "' + req.body.password + '"', (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {
      if (rows[0].count_user == 1) {
        var token = jwt.sign({username: req.body.user_name, page_access: rows[0].page_access}, 'my_secret');
        return res.status(200).json({token:token});
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
      if(lastRow == "-1") {
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
      return res.status(200).json({categories:rows});
    }
  })
});

app.post('/verifytoken', bodyParser.json(), (req, res) => {
  jwt.verify(req.body.token,"my_secret",function(err,tokendata) {
    if(err) {
      res.status(400).json({message: "Unauthorized request"});
    } else {
      res.status(200).json(tokendata);
    }
  });
});