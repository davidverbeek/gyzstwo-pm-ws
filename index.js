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

dbconfig = require("./dbconfig");
const mysql = require('mysql')
const connection = mysql.createConnection({
  host: dbconfig.host,
  user: dbconfig.user,
  password: dbconfig.password,
  database: dbconfig.database
})
connection.connect()

app.post('/auth', bodyParser.json(), (req, res) => {
  // res.json(req.body);
  connection.query('SELECT count(*) AS count_user FROM users WHERE username = "' + req.body.username + '" AND password = "' + req.body.password + '"', (err, rows, fields) => {
    if (err) {
      return res.status(501).json({ message: 'Something went wrong' });
    } else {
      if (rows[0].count_user == 1) {
        var token = jwt.sign({ username: req.body.username }, 'my_secret');
        return res.status(201).json({token:token});
      } else {
        return res.status(501).json({ message: "Invalid user" });
      }
    }
    //console.log('The solution is: ', rows[0].solution)
    //res.json(err);
  })
})