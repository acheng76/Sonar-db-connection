// controllers/userController.js
const oracledb = require("oracledb");
const gonetDBConfig = require("../config/gonetDBConfig");
const fs = require("fs");

const csv = require("csv-parser");

function readDataFromCSV(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => resolve(results))
      .on("error", (error) => reject(error));
  });
}

function joinData(
  dimData,
  factData,
  keys = ["PROD_INSTNC_KEY", "SLA_MNTH"],
  month,
  year
) {
  const joinedData = [];
  dimData.forEach((dimRow) => {
    if (
      new Date(dimRow["SLA_MNTH"]).getFullYear().toString() === year &&
      new Date(dimRow["SLA_MNTH"]).getDate().toString() === month
    ) {
      const factRow = factData.filter((factRow) =>
        keys.map((key) => dimRow[key] == factRow[key]).every(Boolean)
      );
      if (factRow.length > 0) {
        joinedData.push({ ...dimRow, ...factRow[0] });
      }
    }
  });
  return joinedData;
}

async function getPerfReportData(req, res) {
  try {
    const connection = await oracledb.getConnection(gonetDBConfig);
    console.log("Gonet database connected");

    // const slaType = 'SLT-PDV';
    // const startTs = '10/01/2023 00:00:00';
    // const endTs = '10/31/2023 23:59:59';

    //  "http://localhost:5000/report/perfReportData?slaType=SLT-PDV&month=10&year=2023"
    const slaType = req.query.slaType;
    const month = req.query.month;
    const year = req.query.year;

    // const month = 10;
    // const year = 2023;

    const startOfMonth = new Date(year, month - 1, 1, 0, 0, 0);
    const endOfMonth = new Date(year, month, 0, 23, 59, 59);
    const formatDate = (date) => {
      const pad = (num) => (num < 10 ? "0" : "") + num;
      return `${pad(date.getMonth() + 1)}/${pad(
        date.getDate()
      )}/${date.getFullYear()} ${pad(date.getHours())}:${pad(
        date.getMinutes()
      )}:${pad(date.getSeconds())}`;
    };
    const startTs = formatDate(startOfMonth);
    const endTs = formatDate(endOfMonth);

    const sql = fs
      .readFileSync("./src/query/perfReportData.sql", "UTF-8")
      .toString();
    const queryResult = await connection.execute(sql, {
      startTs: startTs,
      endTs: endTs,
      slaType: slaType,
    });

    const transformedData = queryResult.rows.map((row) => {
      const rowObject = {};
      queryResult.metaData.forEach((column, index) => {
        rowObject[column.name] = row[index];
      });
      return rowObject;
    });

    res.json(transformedData);
  } catch (error) {
    console.error(error);
    res.status(500).send("Internal Server Error");
  }
}

async function getAvaiReportData(req, res) {
  try {
    // csvFilePath = './data/test-csv-SA-2month-dim+fact.csv';
    // const data = await readDataFromCSV(csvFilePath);
    // res.json(data);

    dimPath = "./data/availability_dims.csv";
    factPath = "./data/report_sla_srvc_avail_mnthly.csv";

    const dimData = await readDataFromCSV(dimPath);
    const factData = await readDataFromCSV(factPath);

    const INNER_JOIN_KEYS = ["PROD_INSTNC_KEY", "SLA_MNTH"];

    //  "http://localhost:5000/report/avaiReportData?month=10&year=2023"

    const month = req.query.month;
    const year = req.query.year;

    // const month = 10;
    // const year = 2023;

    const data = joinData(dimData, factData, INNER_JOIN_KEYS, month, year);

    res.json(data);
  } catch (error) {
    console.error("Error reading CSV file:", error);
    res.status(500).json({ error: "Internal server error" });
  }
}

module.exports = {
  getPerfReportData,
  getAvaiReportData,
};
