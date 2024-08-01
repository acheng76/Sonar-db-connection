// controllers/userController.js
const oracledb = require("oracledb");
const gonetDBConfig = require("../config/gonetDBConfig");
const fs = require("fs");

const csv = require("csv-parser");
const { join } = require("path");

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

function filterMonthYear(data, key, month, year) {
  return data.filter(
    (row) =>
      row[key]?.split("/")[1] === month &&
      row[key]?.split("/")[2].split(" ")[0] === year
  );
}

// function joinIncidentData(data, incData, key = "PROD_INSTNC_KEY") {
//   const joinedData = [];
//   data.forEach((row) => {
//     const incRows = incData.filter(
//       (incRow) =>
//         row[key] == incRow[key] && incRow["AVAIL_SLA_AFFCT_IND"] === "Y"
//     );
//     joinedData.push({ ...row, TOTAL_INCIDENTS: incRows.length + "" });
//   });
//   return joinedData;
// }

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
    dimPath = "./data/availability_dims.csv";
    factPath = "./data/report_sla_srvc_avail_mnthly.csv";
    incPath = "./data/incident_close.csv";

    const dimData = await readDataFromCSV(dimPath);
    const factData = await readDataFromCSV(factPath);
    // const incData = await readDataFromCSV(incPath);

    const month = req.query.month;
    const year = req.query.year;

    //  "http://localhost:5000/report/avaiReportData?month=10&year=2023"

    const dimDataFilter = filterMonthYear(dimData, "SLA_MNTH", month, year);
    const factDataFilter = filterMonthYear(factData, "SLA_MNTH", month, year);
    // const incDataFilter = filterMonthYear(
    //   incData,
    //   "CLOSE_CUST_TS",
    //   month,
    //   year
    // );

    function joinData(
      dimData,
      factData,
      keys = ["PROD_INSTNC_KEY", "SLA_MNTH"]
    ) {
      const joinedData = [];
      dimData.forEach((dimRow) => {
        const factRows = factData.filter((factRow) =>
          keys.map((key) => dimRow[key] === factRow[key]).every(Boolean)
        );
        if (factRows.length > 0) {
          joinedData.push({ ...dimRow, ...factRows[0] });
        }
      });
      return joinedData;
    }

    const INNER_JOIN_KEYS = ["PROD_INSTNC_KEY", "SLA_MNTH"];

    let data = joinData(dimDataFilter, factDataFilter, INNER_JOIN_KEYS);

    // data = joinIncidentData(data, incDataFilter, "PROD_INSTNC_KEY");
    res.json(data);
  } catch (error) {
    console.error("Error reading CSV file:", error);
    res.status(500).json({ error: "Internal server error" });
  }
}

async function getincidentData(req, res) {
  try {
    incPath = "./data/incident_close.csv";

    const incData = await readDataFromCSV(incPath);

    const month = req.query.month;
    const year = req.query.year;
    const key = req.query.key;

    //  "http://localhost:5000/report/incidentData?month=10&year=2023&key=1235009"

    // const incDataFilter = filterMonthYear(
    //   incData,
    //   "CLOSE_CUST_TS",
    //   month,
    //   year
    // );

    function filterIncidentData(data, key) {
      return data.filter(
        (row) =>
          row["PROD_INSTNC_KEY"] === key && row["AVAIL_SLA_AFFCT_IND"] === "Y"
      );
    }

    const data = filterIncidentData(incData, key);

    const filter = filterMonthYear(data, "CLOSE_CUST_TS", month, year);

    res.json(filter);
  } catch (error) {
    console.error("Error reading CSV file:", error);
    res.status(500).json({ error: "Internal server error" });
  }
}

async function getPerfCoreMMPReportData(req, res) {
  try {
    const connection = await oracledb.getConnection(gonetDBConfig);
    console.log("Gonet database connected");
    const slaType = req.query.slaType;
    const month = req.query.month;
    const year = req.query.year;
    // "http://localhost:5000/report/perfCoreMMPReportData?slaType=SLT-PDV&month=3&year=2024"

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

    const sqlFact = fs
      .readFileSync("./src/query/perfFactMMPData.sql", "UTF-8")
      .toString();
    const sqlDim = fs
      .readFileSync("./src/query/perfDimMMPData.sql", "UTF-8")
      .toString();

    const factData = await connection.execute(sqlFact, {
      startTs: startTs,
      endTs: endTs,
      slaType: slaType,
    });
    const dimData = await connection.execute(sqlDim, {
      startTs: startTs,
      endTs: endTs,
      slaType: slaType,
    });

    const transformeData = (res) =>
      res.rows.map((row) => {
        const rowObject = {};
        res.metaData.forEach((column, index) => {
          rowObject[column.name] = row[index];
        });
        return rowObject;
      });

    const factDataTransform = transformeData(factData);
    const dimDataTransform = transformeData(dimData);

    function joinData(
      dimData,
      factData,
      keys = ["PROD_INSTNC_KEY", "SLA_MNTH"]
    ) {
      const joinedData = [];
      dimData.forEach((dimRow) => {
        const factRows = factData.filter((factRow) =>
          keys
            .map((key) => dimRow[key].toString() === factRow[key].toString())
            .every(Boolean)
        );
        if (factRows.length > 0) {
          joinedData.push({ ...dimRow, ...factRows[0] });
        }
      });
      return joinedData;
    }

    const result = joinData(dimDataTransform, factDataTransform);

    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).send("Internal Server Error");
  }
}

async function getPerfReportDetailMMPData(req, res) {
  try {
    const connection = await oracledb.getConnection(gonetDBConfig);    
    // const slaType = 'SLT-PDV';
    // const startTs = '10/01/2023 00:00:00';
    // const endTs = '10/31/2023 23:59:59';
    //  "http://localhost:5000/report/perfReportDetailMMPData?slaType=SLT-PDV&month=03&year=2024"

    const month = req.query.month;
    const year = req.query.year;

    // const month = 03,04;
    // const year = 2024;

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
      .readFileSync("./src/query/perfReportDetailMMPData.sql", "UTF-8")
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
async function getPerfReportDetailData(req, res) {
  try {
    const connection = await oracledb.getConnection(gonetDBConfig);
    console.log("Gonet getPerfReportDetailData connected");

    // const slaType = 'SLT-PDV';
    // const startTs = '10/01/2023 00:00:00';
    // const endTs = '10/31/2023 23:59:59';

    //  "http://localhost:5000/report/perfReportDetailData?slaType=SLT-PDV&month=05&year=2024"
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
      .readFileSync("./src/query/perfReportDetailData.sql", "UTF-8")
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

module.exports = {
  getPerfReportData,
  getAvaiReportData,
  getincidentData,
  getPerfCoreMMPReportData,
  getPerfReportDetailMMPData,  
  getPerfReportDetailData,
};
