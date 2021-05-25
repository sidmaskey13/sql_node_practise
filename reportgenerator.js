
const express = require('express')
const sqlite3 = require('sqlite3').verbose();
const app = express()
const port = 4000
const request = require('request')
var data


app.set('view engine', 'ejs');
app.set("views", __dirname + "/views");

const db = new sqlite3.Database("petroleum.db", function (err, done) {
    if (err) {
        return console.log(err.message);
    }
    console.log('Database Connected');
})

app.get("/", function (req, res) {
    const sql1 = `SELECT petroleum.name, petroleum.years, AVG(NULLIF(petroleum.Sales,0)) AS Average_Sale FROM
    (SELECT
        *,  (CASE
                WHEN year BETWEEN 2007 AND 2008 THEN '2007 - 2008'
                WHEN year BETWEEN 2009 AND 2010 THEN '2009 - 2010'
                WHEN year BETWEEN 2011 AND 2012 THEN '2011 - 2012'
                WHEN year BETWEEN 2013 AND 2014 THEN '2013 - 2014'
                ELSE 'Others'
            END) AS years
    FROM Petroleum) as petroleum
GROUP BY name,years;`

    const sql2 = `SELECT Name,Country,
SUM (NULLIF(Sales,0)) AS Sum
FROM Petroleum
GROUP BY Country,Name;`

    const sql3 = `SELECT Name,Year,
MIN (NULLIF(Sales,0)) AS Min
FROM Petroleum
GROUP BY Year;`

    db.all(sql1, [], function (err, data1) {
        if (err) {
            return console.error(err.msg);
        }
        db.all(sql2, [], function (err, data2) {
            if (err) {
                return console.error(err.msg);
            }
            db.all(sql3, [], function (err, data3) {
                if (err) {
                    return console.error(err.msg);
                }
                console.log(data3);
                res.render("table.ejs", { task1: data1, task2: data2, task3: data3 });
            })

        })
    });
});


function PullDataFromApi(callback) {
    let options = { json: true };
    let url = "https://raw.githubusercontent.com/younginnovations/internship-challenges/master/programming/petroleum-report/data.json";
    request(url, options, function (err, res) {
        if (err) {
            return err;
        };
        data = res.body;
        callback(data);
    });
}

PullDataFromApi(function (data) {
    createTable().then(function () {
        data.forEach(function (item, index, array) {
            insertIntoTable(array[index].year, array[index].petroleum_product, array[index].sale, array[index].country);
        });
    })
        .catch(function (err) {
            console.log('Error:', err);
        })
})

function createTable() {
    return new Promise(function (resolve, reject) {
        const sql_create = `CREATE TABLE IF NOT EXISTS Petroleum(
        Petroleum_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name VARCHAR(100) NOT NULL,
        Country VARCHAR(100) NOT NULL,
        Sales FLOAT,
        Year VARCHAR(100)
        );`;
        db.run(sql_create, function (err, done) {
            if (err) {
                return console.error(err.message);
            }
            console.log("Petroleum Table created");
            resolve();
        });
    });
}

function insertIntoTable(year, character, sale, country) {
    const sql_query = `INSERT INTO Petroleum (Year,Name,Sales,Country) VALUES(?,?,?,?);`;
    const ref = [year, character, sale, country];
    db.run(sql_query, ref, function (err, done) {
        if (err) {
            return console.error(err.message);
        }
        console.log("Petroleum Data inserted");
    });
}

app.listen(port, () => {
    console.log(`Server started at http://localhost:${port}`)
})
